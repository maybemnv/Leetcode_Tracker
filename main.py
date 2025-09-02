#!/usr/bin/env python3
"""
LeetCode Progress Tracker - Main Entry Point

Automated system to fetch LeetCode solved problems and sync to Google Sheets
with comprehensive analytics and progress tracking.
"""

import os
import sys
import logging
import argparse
import json
from pathlib import Path
from datetime import datetime

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config.setting import load_config, ConfigManager
from src.sync_manager import SyncManager

def setup_logging(log_level: str = 'INFO', log_file: str = None):
    """
    Setup logging configuration
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
    """
    # Create logs directory if it doesn't exist
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file) if log_file else logging.NullHandler()
        ]
    )
    
    # Set specific logger levels
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized at level: {log_level}")
    
    if log_file:
        logger.info(f"Log file: {log_file}")

def create_env_template():
    """Create environment variables template file"""
    config = ConfigManager()
    config.create_env_template()
    print("✅ Environment template created at .env.example")
    print("📝 Please copy .env.example to .env and fill in your values")

def test_connections():
    """Test connections to LeetCode and Google Sheets"""
    try:
        print("🔍 Testing connections...")
        config = load_config()
        sync_manager = SyncManager(config.config)
        
        results = sync_manager.test_connections()
        
        print("\n=== Connection Test Results ===")
        for service, status in results.items():
            status_icon = "✅" if status else "❌"
            print(f"{status_icon} {service.replace('_', ' ').title()}: {'PASSED' if status else 'FAILED'}")
        
        if all(results.values()):
            print("\n🎉 All connections successful!")
            return True
        else:
            print("\n⚠️  Some connections failed. Please check your configuration.")
            return False
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def sync_data(force_full: bool = False, incremental: bool = False):
    """
    Synchronize LeetCode data to Google Sheets
    
    Args:
        force_full: Force full synchronization
        incremental: Perform incremental sync only
    """
    try:
        print("🔄 Starting data synchronization...")
        config = load_config()
        sync_manager = SyncManager(config.config)
        
        if incremental:
            print("📈 Performing incremental synchronization...")
            success = sync_manager.incremental_sync()
        else:
            print("📊 Performing full synchronization...")
            success = sync_manager.sync_all_data(force_full_sync=force_full)
        
        if success:
            print("✅ Synchronization completed successfully!")
            
            # Get sync status
            status = sync_manager.get_sync_status()
            stats = status.get('sync_stats', {})
            
            print(f"\n📊 Sync Summary:")
            print(f"   Total Problems: {stats.get('total_problems', 0)}")
            print(f"   New Problems: {stats.get('new_problems', 0)}")
            print(f"   Last Sync: {stats.get('last_sync', 'Unknown')}")
            
            return True
        else:
            print("❌ Synchronization failed!")
            return False
            
    except Exception as e:
        print(f"❌ Synchronization error: {e}")
        return False

def show_status():
    """Show current synchronization status"""
    try:
        print("📊 Getting synchronization status...")
        config = load_config()
        sync_manager = SyncManager(config.config)
        
        status = sync_manager.get_sync_status()
        
        print("\n=== LeetCode Tracker Status ===")
        
        # Connection status
        connections = status.get('connections', {})
        print(f"\n🔌 Connections:")
        for service, connected in connections.items():
            status_icon = "✅" if connected else "❌"
            print(f"   {status_icon} {service.replace('_', ' ').title()}")
        
        # Sync stats
        stats = status.get('sync_stats', {})
        print(f"\n📈 Sync Statistics:")
        print(f"   Total Problems: {stats.get('total_problems', 0)}")
        print(f"   New Problems: {stats.get('new_problems', 0)}")
        print(f"   Updated Problems: {stats.get('updated_problems', 0)}")
        print(f"   Errors: {stats.get('errors', 0)}")
        print(f"   Last Sync: {stats.get('last_sync', 'Never')}")
        
        # Component status
        components = status.get('components_initialized', {})
        print(f"\n🔧 Components:")
        print(f"   Configuration: {'✅' if status.get('config_loaded') else '❌'}")
        print(f"   All Components: {'✅' if components else '❌'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to get status: {e}")
        return False

def backup_data(backup_path: str = None):
    """
    Create a backup of current data
    
    Args:
        backup_path: Optional backup file path
    """
    try:
        print("💾 Creating data backup...")
        config = load_config()
        sync_manager = SyncManager(config.config)
        
        success = sync_manager.backup_data(backup_path)
        
        if success:
            print("✅ Backup created successfully!")
            return True
        else:
            print("❌ Backup failed!")
            return False
            
    except Exception as e:
        print(f"❌ Backup error: {e}")
        return False

def restore_data(backup_path: str):
    """
    Restore data from backup
    
    Args:
        backup_path: Path to backup file
    """
    try:
        if not os.path.exists(backup_path):
            print(f"❌ Backup file not found: {backup_path}")
            return False
        
        print(f"🔄 Restoring data from backup: {backup_path}")
        config = load_config()
        sync_manager = SyncManager(config.config)
        
        success = sync_manager.restore_data(backup_path)
        
        if success:
            print("✅ Data restored successfully!")
            return True
        else:
            print("❌ Data restoration failed!")
            return False
            
    except Exception as e:
        print(f"❌ Restore error: {e}")
        return False

def cleanup_data(days_to_keep: int = 365):
    """
    Clean up old data
    
    Args:
        days_to_keep: Number of days of data to keep
    """
    try:
        print(f"🧹 Cleaning up data older than {days_to_keep} days...")
        config = load_config()
        sync_manager = SyncManager(config.config)
        
        success = sync_manager.cleanup_old_data(days_to_keep)
        
        if success:
            print("✅ Data cleanup completed successfully!")
            return True
        else:
            print("❌ Data cleanup failed!")
            return False
            
    except Exception as e:
        print(f"❌ Cleanup error: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="LeetCode Progress Tracker - Automated sync to Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test connections
  python main.py test
  
  # Full synchronization
  python main.py sync
  
  # Incremental synchronization
  python main.py sync --incremental
  
  # Show status
  python main.py status
  
  # Create backup
  python main.py backup
  
  # Restore from backup
  python main.py restore backup_file.json
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Test connections command
    subparsers.add_parser('test', help='Test connections to LeetCode and Google Sheets')
    
    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Synchronize LeetCode data to Google Sheets')
    sync_parser.add_argument('--force-full', action='store_true', help='Force full synchronization')
    sync_parser.add_argument('--incremental', action='store_true', help='Perform incremental sync only')
    
    # Status command
    subparsers.add_parser('status', help='Show current synchronization status')
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create backup of current data')
    backup_parser.add_argument('--path', help='Backup file path')
    
    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore data from backup')
    restore_parser.add_argument('backup_path', help='Path to backup file')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old data')
    cleanup_parser.add_argument('--days', type=int, default=365, help='Days of data to keep')
    
    # Setup command
    subparsers.add_parser('setup', help='Create environment template file')
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Setup logging
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_file = os.getenv('LOG_FILE', 'logs/leetcode_tracker.log')
    setup_logging(log_level, log_file)
    
    # Execute command
    try:
        if args.command == 'test':
            test_connections()
            
        elif args.command == 'sync':
            if args.incremental:
                sync_data(incremental=True)
            else:
                sync_data(force_full=args.force_full)
                
        elif args.command == 'status':
            show_status()
            
        elif args.command == 'backup':
            backup_data(args.path)
            
        elif args.command == 'restore':
            restore_data(args.backup_path)
            
        elif args.command == 'cleanup':
            cleanup_data(args.days)
            
        elif args.command == 'setup':
            create_env_template()
            
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
