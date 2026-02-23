#!/usr/bin/env python3
"""
Fundraising/crowdfunding backend for BlackRoad.
Manages campaigns, pledges, rewards, and fundraising analytics.
"""

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from enum import Enum


class CampaignStatus(Enum):
    """Campaign status enumeration."""
    ACTIVE = "active"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RewardTier(Enum):
    """Reward tier definitions."""
    SUPPORTER = ("supporter", 5)
    BACKER = ("backer", 25)
    CHAMPION = ("champion", 100)
    FOUNDER = ("founder", 500)


@dataclass
class Campaign:
    """Represents a fundraising campaign."""
    id: str
    title: str
    creator: str
    category: str
    goal_usd: float
    raised_usd: float
    backers: int
    deadline: str  # ISO format datetime
    status: str  # active, success, failed, cancelled
    description: str = ""


@dataclass
class Pledge:
    """Represents a pledge to a campaign."""
    id: str
    campaign_id: str
    backer: str
    amount_usd: float
    reward_tier: str
    ts: str  # ISO format datetime


class FundraisingPlatform:
    """Manages fundraising campaigns and pledges."""
    
    VALID_CATEGORIES = {"tech", "art", "music", "games", "film", "community", "science"}
    REWARD_TIERS = {tier.value[0]: tier.value[1] for tier in RewardTier}
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize platform with SQLite database."""
        if db_path is None:
            db_path = str(Path.home() / ".blackroad" / "fundraising.db")
        
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Create campaigns table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                creator TEXT NOT NULL,
                category TEXT NOT NULL,
                goal_usd REAL NOT NULL,
                raised_usd REAL DEFAULT 0,
                backers INTEGER DEFAULT 0,
                deadline TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                description TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create pledges table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pledges (
                id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                backer TEXT NOT NULL,
                amount_usd REAL NOT NULL,
                reward_tier TEXT NOT NULL,
                ts TEXT NOT NULL,
                refunded INTEGER DEFAULT 0,
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _get_conn(self) -> sqlite3.Connection:
        """Get database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _generate_id(self, prefix: str) -> str:
        """Generate unique ID."""
        import hashlib
        import time
        ts = str(time.time()).encode()
        return f"{prefix}_{hashlib.md5(ts).hexdigest()[:8]}"
    
    def create_campaign(
        self,
        title: str,
        creator: str,
        category: str,
        goal_usd: float,
        deadline_days: int,
        description: str = ""
    ) -> Campaign:
        """Create a new fundraising campaign."""
        if category not in self.VALID_CATEGORIES:
            raise ValueError(f"Invalid category. Must be one of: {self.VALID_CATEGORIES}")
        
        if goal_usd <= 0:
            raise ValueError("Goal must be positive")
        
        campaign_id = self._generate_id("camp")
        deadline = (datetime.now() + timedelta(days=deadline_days)).isoformat()
        
        campaign = Campaign(
            id=campaign_id,
            title=title,
            creator=creator,
            category=category,
            goal_usd=goal_usd,
            raised_usd=0,
            backers=0,
            deadline=deadline,
            status=CampaignStatus.ACTIVE.value,
            description=description
        )
        
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO campaigns 
            (id, title, creator, category, goal_usd, deadline, status, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            campaign.id, campaign.title, campaign.creator, campaign.category,
            campaign.goal_usd, campaign.deadline, campaign.status, campaign.description
        ))
        conn.commit()
        conn.close()
        
        return campaign
    
    def pledge(
        self,
        campaign_id: str,
        backer: str,
        amount_usd: float,
        reward_tier: str = "supporter"
    ) -> Pledge:
        """Make a pledge to a campaign."""
        if reward_tier not in self.REWARD_TIERS:
            raise ValueError(f"Invalid reward tier. Must be one of: {list(self.REWARD_TIERS.keys())}")
        
        min_amount = self.REWARD_TIERS[reward_tier]
        if amount_usd < min_amount:
            raise ValueError(f"Amount ${amount_usd} is below minimum for {reward_tier} (${min_amount})")
        
        conn = self._get_conn()
        cur = conn.cursor()
        
        # Get campaign
        cur.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
        campaign_row = cur.fetchone()
        if not campaign_row:
            raise ValueError(f"Campaign not found: {campaign_id}")
        
        if campaign_row["status"] != CampaignStatus.ACTIVE.value:
            raise ValueError(f"Cannot pledge to {campaign_row['status']} campaign")
        
        # Create pledge
        pledge_id = self._generate_id("pledge")
        ts = datetime.now().isoformat()
        
        pledge = Pledge(
            id=pledge_id,
            campaign_id=campaign_id,
            backer=backer,
            amount_usd=amount_usd,
            reward_tier=reward_tier,
            ts=ts
        )
        
        # Insert pledge
        cur.execute("""
            INSERT INTO pledges 
            (id, campaign_id, backer, amount_usd, reward_tier, ts)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (pledge.id, pledge.campaign_id, pledge.backer, pledge.amount_usd, pledge.reward_tier, pledge.ts))
        
        # Update campaign stats
        cur.execute("""
            UPDATE campaigns 
            SET raised_usd = raised_usd + ?,
                backers = backers + 1
            WHERE id = ?
        """, (amount_usd, campaign_id))
        
        conn.commit()
        conn.close()
        
        return pledge
    
    def get_campaigns(
        self,
        category: Optional[str] = None,
        status: str = "active",
        sort_by: str = "raised"
    ) -> List[Campaign]:
        """Get campaigns with optional filtering and sorting."""
        conn = self._get_conn()
        cur = conn.cursor()
        
        query = "SELECT * FROM campaigns WHERE status = ?"
        params = [status]
        
        if category:
            query += " AND category = ?"
            params.append(category)
        
        # Sort options
        if sort_by == "raised":
            query += " ORDER BY raised_usd DESC"
        elif sort_by == "deadline":
            query += " ORDER BY deadline ASC"
        elif sort_by == "created":
            query += " ORDER BY created_at DESC"
        else:
            query += " ORDER BY raised_usd DESC"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        
        return [Campaign(**dict(row)) for row in rows]
    
    def get_campaign(self, campaign_id: str) -> Dict:
        """Get campaign with progress info."""
        conn = self._get_conn()
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
        campaign_row = cur.fetchone()
        if not campaign_row:
            raise ValueError(f"Campaign not found: {campaign_id}")
        
        campaign_dict = dict(campaign_row)
        
        # Calculate progress percentage
        campaign_dict["progress_pct"] = min(
            100,
            (campaign_dict["raised_usd"] / campaign_dict["goal_usd"] * 100) if campaign_dict["goal_usd"] > 0 else 0
        )
        
        # Calculate days left
        deadline = datetime.fromisoformat(campaign_dict["deadline"])
        days_left = (deadline - datetime.now()).days
        campaign_dict["days_left"] = max(0, days_left)
        
        # Get backer list
        cur.execute("""
            SELECT DISTINCT backer FROM pledges 
            WHERE campaign_id = ? AND refunded = 0
            ORDER BY ts DESC
        """, (campaign_id,))
        backers = [row[0] for row in cur.fetchall()]
        campaign_dict["backers_list"] = backers
        
        conn.close()
        return campaign_dict
    
    def check_deadlines(self) -> Tuple[int, int]:
        """
        Marks expired campaigns as success/failed based on goal.
        Returns (success_count, failed_count).
        """
        conn = self._get_conn()
        cur = conn.cursor()
        
        now = datetime.now().isoformat()
        
        # Find expired active campaigns
        cur.execute("""
            SELECT id, goal_usd, raised_usd FROM campaigns
            WHERE status = ? AND deadline < ?
        """, (CampaignStatus.ACTIVE.value, now))
        
        expired = cur.fetchall()
        success_count = 0
        failed_count = 0
        
        for campaign_id, goal_usd, raised_usd in expired:
            if raised_usd >= goal_usd:
                new_status = CampaignStatus.SUCCESS.value
                success_count += 1
            else:
                new_status = CampaignStatus.FAILED.value
                failed_count += 1
            
            cur.execute("""
                UPDATE campaigns SET status = ? WHERE id = ?
            """, (new_status, campaign_id))
        
        conn.commit()
        conn.close()
        return success_count, failed_count
    
    def get_stats(self) -> Dict[str, any]:
        """Get platform statistics."""
        conn = self._get_conn()
        cur = conn.cursor()
        
        # Total raised
        cur.execute("SELECT SUM(raised_usd) as total FROM campaigns WHERE status != ?", 
                   (CampaignStatus.CANCELLED.value,))
        total_raised = cur.fetchone()[0] or 0
        
        # Total campaigns
        cur.execute("SELECT COUNT(*) as count FROM campaigns")
        total_campaigns = cur.fetchone()[0]
        
        # Success rate
        cur.execute("SELECT COUNT(*) as count FROM campaigns WHERE status = ?", 
                   (CampaignStatus.SUCCESS.value,))
        successful = cur.fetchone()[0]
        success_rate = (successful / total_campaigns * 100) if total_campaigns > 0 else 0
        
        # Average goal
        cur.execute("SELECT AVG(goal_usd) as avg FROM campaigns")
        avg_goal = cur.fetchone()[0] or 0
        
        conn.close()
        
        return {
            "total_raised_usd": total_raised,
            "total_campaigns": total_campaigns,
            "success_rate_pct": round(success_rate, 2),
            "avg_goal_usd": round(avg_goal, 2)
        }
    
    def refund_campaign(self, campaign_id: str) -> int:
        """
        Refund all pledges for a failed campaign.
        Returns number of pledges refunded.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        
        # Get campaign
        cur.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
        campaign_row = cur.fetchone()
        if not campaign_row:
            raise ValueError(f"Campaign not found: {campaign_id}")
        
        if campaign_row["status"] != CampaignStatus.FAILED.value:
            raise ValueError(f"Can only refund failed campaigns (status: {campaign_row['status']})")
        
        # Mark pledges as refunded
        cur.execute("""
            UPDATE pledges SET refunded = 1 
            WHERE campaign_id = ? AND refunded = 0
        """, (campaign_id,))
        
        refund_count = cur.rowcount
        
        # Reset campaign stats
        if refund_count > 0:
            cur.execute("""
                UPDATE campaigns 
                SET raised_usd = 0, backers = 0
                WHERE id = ?
            """, (campaign_id,))
        
        conn.commit()
        conn.close()
        
        return refund_count


def main():
    parser = argparse.ArgumentParser(description="BlackRoad fundraising platform")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # list command
    list_parser = subparsers.add_parser("list", help="List campaigns")
    list_parser.add_argument("--category", help="Filter by category")
    list_parser.add_argument("--status", default="active", help="Filter by status")
    
    # create command
    create_parser = subparsers.add_parser("create", help="Create a campaign")
    create_parser.add_argument("title", help="Campaign title")
    create_parser.add_argument("creator", help="Creator name")
    create_parser.add_argument("category", help="Category")
    create_parser.add_argument("goal_usd", type=float, help="Funding goal in USD")
    create_parser.add_argument("--days", type=int, default=30, help="Days to deadline")
    create_parser.add_argument("--description", default="", help="Campaign description")
    
    # pledge command
    pledge_parser = subparsers.add_parser("pledge", help="Make a pledge")
    pledge_parser.add_argument("campaign_id", help="Campaign ID")
    pledge_parser.add_argument("backer", help="Backer name")
    pledge_parser.add_argument("amount_usd", type=float, help="Pledge amount in USD")
    pledge_parser.add_argument("--tier", default="supporter", help="Reward tier")
    
    # view command
    view_parser = subparsers.add_parser("view", help="View campaign details")
    view_parser.add_argument("campaign_id", help="Campaign ID")
    
    # stats command
    subparsers.add_parser("stats", help="View platform statistics")
    
    # check command
    subparsers.add_parser("check", help="Check deadlines and update statuses")
    
    args = parser.parse_args()
    platform = FundraisingPlatform()
    
    try:
        if args.command == "list":
            campaigns = platform.get_campaigns(category=args.category, status=args.status)
            print(f"Found {len(campaigns)} campaigns")
            for campaign in campaigns:
                progress = (campaign.raised_usd / campaign.goal_usd * 100) if campaign.goal_usd > 0 else 0
                print(f"  [{campaign.id}] {campaign.title} ({campaign.category})")
                print(f"      ${campaign.raised_usd:.2f}/${campaign.goal_usd:.2f} ({progress:.1f}%) - {campaign.backers} backers")
        
        elif args.command == "create":
            campaign = platform.create_campaign(
                title=args.title,
                creator=args.creator,
                category=args.category,
                goal_usd=args.goal_usd,
                deadline_days=args.days,
                description=args.description
            )
            print(f"Campaign created: {campaign.id}")
            print(f"  Title: {campaign.title}")
            print(f"  Goal: ${campaign.goal_usd:.2f}")
            print(f"  Deadline: {campaign.deadline}")
        
        elif args.command == "pledge":
            pledge = platform.pledge(
                campaign_id=args.campaign_id,
                backer=args.backer,
                amount_usd=args.amount_usd,
                reward_tier=args.tier
            )
            print(f"Pledge recorded: {pledge.id}")
            print(f"  Amount: ${pledge.amount_usd:.2f}")
            print(f"  Tier: {pledge.reward_tier}")
        
        elif args.command == "view":
            campaign = platform.get_campaign(args.campaign_id)
            print(f"Campaign: {campaign['title']}")
            print(f"  Creator: {campaign['creator']}")
            print(f"  Raised: ${campaign['raised_usd']:.2f}/${campaign['goal_usd']:.2f} ({campaign['progress_pct']:.1f}%)")
            print(f"  Backers: {campaign['backers']}")
            print(f"  Status: {campaign['status']}")
            print(f"  Days left: {campaign['days_left']}")
            if campaign['backers_list']:
                print(f"  Recent backers: {', '.join(campaign['backers_list'][:5])}")
        
        elif args.command == "stats":
            stats = platform.get_stats()
            print("Platform Statistics:")
            print(f"  Total Raised: ${stats['total_raised_usd']:.2f}")
            print(f"  Total Campaigns: {stats['total_campaigns']}")
            print(f"  Success Rate: {stats['success_rate_pct']:.1f}%")
            print(f"  Average Goal: ${stats['avg_goal_usd']:.2f}")
        
        elif args.command == "check":
            success, failed = platform.check_deadlines()
            print(f"Deadline check complete: {success} succeeded, {failed} failed")
        
        else:
            parser.print_help()
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
