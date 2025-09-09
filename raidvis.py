import os
import shutil
import threading
import time
import queue
import datetime
from typing import List, Dict, Optional

log_queue = queue.Queue()
logging_active = True

class Logger:
    """Handles system logging both to console and file"""
    
    def __init__(self, log_file="system.log"):
        self.log_file = log_file
        self.log_thread = threading.Thread(target=self._logging_worker, daemon=True)
        self.log_thread.start()
    
    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{timestamp}] {level}: {message}"
        log_queue.put(formatted_msg)
    
    def _logging_worker(self):
        with open(self.log_file, 'a') as f:
            while logging_active:
                try:
                    message = log_queue.get(timeout=0.1)
                    print(message)
                    f.write(message + '\n')
                    f.flush()
                except queue.Empty:
                    continue
# ---------------------------------------------------------------------
# RAID Array and Drive Classes
# ---------------------------------------------------------------------
class Drive:
    """Represents a single drive in the RAID array"""
    
    def __init__(self, drive_id: int, folder_path: str):
        self.drive_id = drive_id
        self.folder_path = folder_path
        self.file_path = os.path.join(folder_path, f"disk_{drive_id}")
        self.is_active = True
        self.sectors = {}
        self.metadata = {
            "drive_id": drive_id,
            "creation_time": datetime.datetime.now().isoformat(),
            "status": "active",
            "total_sectors": 0,
            "used_sectors": 0
        }
        self.create_drive_file()
    
    def create_drive_file(self):
        """Initialize the drive file with header and metadata"""
        try:
            with open(self.file_path, 'w') as f:
                f.write(f"{'='*50}\n")
                f.write(f"RAID DRIVE {self.drive_id} - DEMONSTRATION FILE\n")
                f.write(f"{'='*50}\n\n")
                f.write("METADATA:\n")
                f.write(f"Drive ID: {self.drive_id}\n")
                f.write(f"Status: {self.metadata['status']}\n")
                f.write(f"Created: {self.metadata['creation_time']}\n")
                f.write(f"Part of RAID: [Will be updated]\n")
                f.write(f"Position in RAID: [Will be updated]\n")
                f.write(f"Rebuild Rate: N/A\n\n")
                f.write("BLOCK DIAGRAM:\n")
                f.write("+--------+--------+--------+--------+\n")
                f.write("| Sector | Block  | Type   | Data   |\n")
                f.write("+--------+--------+--------+--------+\n")
        except Exception as e:
            logger.log(f"Error creating drive file {self.file_path}: {e}", "ERROR")
    
    def write_sector(self, sector: int, data: str, block_type: str = "DATA"):
        """Write data to a specific sector"""
        if not self.is_active:
            raise Exception(f"Drive {self.drive_id} is not active")
        
        self.sectors[sector] = {"data": data, "type": block_type}
        self.metadata["used_sectors"] = len(self.sectors)
        self._update_file()
        logger.log(f"Drive {self.drive_id}: Written '{data}' to sector {sector} as {block_type}")
    
    def read_sector(self, sector: int) -> Optional[str]:
        """Read data from a specific sector"""
        if not self.is_active:
            raise Exception(f"Drive {self.drive_id} is not active")
        
        if sector in self.sectors:
            return self.sectors[sector]["data"]
        return None
    
    def mark_failed(self):
        """Mark drive as failed"""
        self.is_active = False
        self.metadata["status"] = "failed"
        logger.log(f"Drive {self.drive_id}: DRIVE FAILURE DETECTED", "ERROR")
    
    def _update_file(self):
        """Update the drive file with current data"""
        try:
            with open(self.file_path, 'w') as f:
                f.write(f"{'='*50}\n")
                f.write(f"RAID DRIVE {self.drive_id} - DEMONSTRATION FILE\n")
                f.write(f"{'='*50}\n\n")
                f.write("METADATA:\n")
                f.write(f"Drive ID: {self.drive_id}\n")
                f.write(f"Status: {self.metadata['status']}\n")
                f.write(f"Created: {self.metadata['creation_time']}\n")
                f.write(f"Used Sectors: {self.metadata['used_sectors']}\n\n")
                f.write("BLOCK DIAGRAM:\n")
                f.write("+--------+--------+--------+--------+\n")
                f.write("| Sector | Block  | Type   | Data   |\n")
                f.write("+--------+--------+--------+--------+\n")
                
                for sector in sorted(self.sectors.keys()):
                    sector_data = self.sectors[sector]
                    data_preview = sector_data["data"][:8] if len(sector_data["data"]) > 8 else sector_data["data"]
                    f.write(f"|   {sector:2d}   | Block{sector:2d} | {sector_data['type']:6s} | {data_preview:6s} |\n")
                
                f.write("+--------+--------+--------+--------+\n")
        except Exception as e:
            logger.log(f"Error updating drive file {self.file_path}: {e}", "ERROR")


# TODO: Implement RAID levels 0, 1, and 5 with their specific logic

# ---------------------------------------------------------------------
# Interactive mode implementation
# ---------------------------------------------------------------------


def interactive_mode(raid: RAIDArray):
    """Interactive mode for RAID demonstration"""
    while True:
        print(f"\n{'='*50}")
        print(f"RAID-{raid.raid_level} Interactive Demo")
        print(f"{'='*50}")
        print("Options:")
        print("1. Write data to RAID")
        print("2. Remove drive (simulate failure)")
        print("3. Add drive to RAID")
        print("4. Edit/overwrite current data")
        print("5. View RAID status and drive contents")
        print("6. Exit RAID demo")
        print()
        
        choice = input("Enter your choice (1-6): ").strip()
        
        if choice == '1':
            data = input("Enter data to write: ")
            if data:
                raid.write_data(data)
                print("Press Enter to continue...")
                input()
        
        elif choice == '2':
            raid.display_status()
            try:
                drive_id = int(input("Enter drive ID to remove: "))
                raid.remove_drive(drive_id)
            except ValueError:
                print("Invalid drive ID")
            print("Press Enter to continue...")
            input()
        
        elif choice == '3':
            drive_id = raid.add_drive()
            print(f"Added drive {drive_id}")
            print("Press Enter to continue...")
            input()
        
        elif choice == '4':
            data = input("Enter new data to write: ")
            if data:
                raid.write_data(data)
                print("Press Enter to continue...")
                input()
        
        elif choice == '5':
            raid.display_status()
            print("\nDrive files created in folder:", raid.folder_path)
            print("You can view the disk_X files to see detailed block layouts")
            print("Press Enter to continue...")
            input()
        
        elif choice == '6':
            break
        
        else:
            print("Invalid choice. Please try again.")

# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------

def main():
    """Main program entry point"""
    global logging_active
    
    # Initialize logger
    global logger
    logger = Logger()
    
    print("RAID Visualization Teaching Tool")
    print("=" * 40)
    logger.log("RAID Teaching Tool started")
    
    try:
        while True:
            print("\nAvailable RAID Levels:")
            print("0 - RAID-0 (Striping)")
            print("1 - RAID-1 (Mirroring)")
            print("5 - RAID-5 (Striping with Parity)")
            print("q - Quit")
            
            choice = input("\nSelect RAID level for demonstration: ").strip().lower()
            
            if choice == 'q':
                break
            
            try:
                raid_level = int(choice)
                if raid_level in [0, 1, 5]:
                    raid = RAIDArray(raid_level)
                    interactive_mode(raid)
                    raid.cleanup()
                else:
                    print("Invalid RAID level. Please choose 0, 1, or 5.")
            except ValueError:
                print("Invalid input. Please enter a number or 'q' to quit.")
    
    except KeyboardInterrupt:
        print("\n\nShutting down...")
    
    finally: # Delay is here if you are wondering
        logging_active = False
        logger.log("RAID Teaching Tool shutting down")
        time.sleep(0.5)  
        
if __name__ == "__main__":
    main()