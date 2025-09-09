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

class RAIDArray:
    """Main RAID array implementation"""
    
    def __init__(self, raid_level: int):
        self.raid_level = raid_level
        self.drives = []
        self.folder_path = f"raid_{raid_level}"
        self.rebuild_active = False
        self.rebuild_thread = None
        self.current_sector = 0
        
        # RAID configuration
        self.raid_configs = {
            0: {"min_drives": 2, "name": "RAID-0 (Striping)", "fault_tolerance": 0},
            1: {"min_drives": 2, "name": "RAID-1 (Mirroring)", "fault_tolerance": 1},
            5: {"min_drives": 3, "name": "RAID-5 (Striping with Parity)", "fault_tolerance": 1}
        }
        
        self.initialize_raid()
    
    def initialize_raid(self):
        """Initialize the RAID array"""
        logger.log(f"Initializing {self.raid_configs[self.raid_level]['name']}")
        
        # Create folder structure
        if os.path.exists(self.folder_path):
            shutil.rmtree(self.folder_path)
        os.makedirs(self.folder_path)
        
        # Create minimum required drives
        min_drives = self.raid_configs[self.raid_level]['min_drives']
        for i in range(min_drives):
            self.add_drive()
        
        logger.log(f"RAID-{self.raid_level} initialized with {len(self.drives)} drives")
    
    def add_drive(self):
        """Add a new drive to the RAID array"""
        drive_id = len(self.drives)
        new_drive = Drive(drive_id, self.folder_path)
        self.drives.append(new_drive)
        logger.log(f"Added drive {drive_id} to RAID-{self.raid_level}")
        return drive_id
    
    def remove_drive(self, drive_id: int):
        """Remove a drive from the RAID array (simulate failure)"""
        if drive_id < len(self.drives) and self.drives[drive_id].is_active:
            self.drives[drive_id].mark_failed()
            
            # Check if RAID is still functional
            active_drives = sum(1 for d in self.drives if d.is_active)
            fault_tolerance = self.raid_configs[self.raid_level]['fault_tolerance']
            
            if active_drives < (len(self.drives) - fault_tolerance):
                logger.log("RAID FAILURE: Not enough drives to maintain data integrity!", "ERROR")
            else:
                logger.log(f"RAID-{self.raid_level} operating in degraded mode", "WARN")
                
                # Ask if user wants to add a replacement
                print(f"\nRAID is operating in degraded mode.")
                print("Would you like to add a replacement drive and start rebuild? (y/n): ", end="")
                choice = input().lower()
                if choice == 'y':
                    replacement_id = self.add_drive()
                    self.start_rebuild(drive_id, replacement_id)
        else:
            logger.log(f"Cannot remove drive {drive_id}: drive not found or already inactive", "ERROR")
    
    def write_data(self, data: str):
        """Write data to the RAID array based on RAID level"""
        logger.log(f"Writing data: '{data}' to RAID-{self.raid_level}")
        
        if self.raid_level == 0:
            self._write_raid0(data)
        elif self.raid_level == 1:
            self._write_raid1(data)
        elif self.raid_level == 5:
            self._write_raid5(data)
    
    def _write_raid0(self, data: str):
        """RAID-0 striping implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if not active_drives:
            logger.log("No active drives available", "ERROR")
            return
        
        # Split data into blocks and stripe across drives
        for i, char in enumerate(data):
            drive_index = i % len(active_drives)
            active_drives[drive_index].write_sector(self.current_sector, char, "DATA")
            time.sleep(0.2)  # Slow down for demonstration
        
        self.current_sector += 1
        logger.log("RAID-0 write operation completed")
    
    def _write_raid1(self, data: str):
        """RAID-1 mirroring implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if not active_drives:
            logger.log("No active drives available", "ERROR")
            return
        
        # Write same data to all drives
        for char in data:
            for drive in active_drives:
                drive.write_sector(self.current_sector, char, "DATA")
                time.sleep(0.1)  # Slow down for demonstration
        
        self.current_sector += 1
        logger.log("RAID-1 write operation completed")
    
    def _write_raid5(self, data: str):
        """RAID-5 striping with parity implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if len(active_drives) < 3:
            logger.log("RAID-5 requires at least 3 active drives", "ERROR")
            return
        
        # Calculate parity drive position (rotating)
        parity_drive_index = self.current_sector % len(active_drives)
        
        # Write data blocks to non-parity drives
        data_drives = [i for i in range(len(active_drives)) if i != parity_drive_index]
        parity_data = ""
        
        for i, char in enumerate(data):
            if i < len(data_drives):
                drive_index = data_drives[i]
                active_drives[drive_index].write_sector(self.current_sector, char, "DATA")
                parity_data += char
                time.sleep(0.2)
        
        # Calculate and write parity
        time.sleep(0.5)  # Simulate parity calculation time
        parity_char = self._calculate_parity(parity_data)
        active_drives[parity_drive_index].write_sector(self.current_sector, parity_char, "PARITY")
        
        logger.log(f"Parity calculated and stored on drive {active_drives[parity_drive_index].drive_id}")
        self.current_sector += 1
        logger.log("RAID-5 write operation completed")
    
    def _calculate_parity(self, data: str) -> str:
        """Simple XOR parity calculation for demonstration"""
        parity = 0
        for char in data:
            parity ^= ord(char)
        return chr(parity % 256)  # Keep it printable
    
    def start_rebuild(self, failed_drive_id: int, replacement_drive_id: int):
        """Start the rebuild process in a separate thread"""
        if not self.rebuild_active:
            self.rebuild_active = True
            self.rebuild_thread = threading.Thread(
                target=self._rebuild_worker,
                args=(failed_drive_id, replacement_drive_id),
                daemon=True
            )
            self.rebuild_thread.start()
            logger.log(f"Started rebuild: Drive {failed_drive_id} -> Drive {replacement_drive_id}")
    
    def _rebuild_worker(self, failed_drive_id: int, replacement_drive_id: int):
        """Rebuild worker thread"""
        try:
            failed_drive = self.drives[failed_drive_id]
            replacement_drive = self.drives[replacement_drive_id]
            
            logger.log("REBUILD: Starting drive rebuild process")
            
            # Simulate rebuild for each sector that was written
            for sector in range(self.current_sector):
                if not self.rebuild_active:
                    break
                
                # Simulate rebuild delay
                time.sleep(1.0)
                
                if self.raid_level == 1:
                    # RAID-1: Copy from mirror
                    for drive in self.drives:
                        if drive.is_active and drive.drive_id != failed_drive_id:
                            if sector in drive.sectors:
                                data = drive.sectors[sector]["data"]
                                replacement_drive.write_sector(sector, data, "REBUILT")
                                break
                
                elif self.raid_level == 5:
                    # RAID-5: Reconstruct from remaining drives using parity
                    active_drives = [d for d in self.drives if d.is_active and d.drive_id != failed_drive_id]
                    if len(active_drives) >= 2:
                        # Simple reconstruction for demonstration
                        data = "R"  # Reconstructed data placeholder
                        replacement_drive.write_sector(sector, data, "REBUILT")
                
                progress = ((sector + 1) / self.current_sector) * 100
                logger.log(f"REBUILD: Progress {progress:.1f}% - Sector {sector}")
            
            logger.log("REBUILD: Drive rebuild completed successfully")
            
        except Exception as e:
            logger.log(f"REBUILD: Error during rebuild - {e}", "ERROR")
        
        finally:
            self.rebuild_active = False
    
    def display_status(self):
        """Display current RAID status"""
        print(f"\n{'='*60}")
        print(f"RAID-{self.raid_level} STATUS")
        print(f"{'='*60}")
        print(f"Configuration: {self.raid_configs[self.raid_level]['name']}")
        print(f"Total Drives: {len(self.drives)}")
        print(f"Active Drives: {sum(1 for d in self.drives if d.is_active)}")
        print(f"Failed Drives: {sum(1 for d in self.drives if not d.is_active)}")
        print(f"Current Sector: {self.current_sector}")
        print(f"Rebuild Active: {'Yes' if self.rebuild_active else 'No'}")
        print()
        
        for drive in self.drives:
            status = "ACTIVE" if drive.is_active else "FAILED"
            print(f"Drive {drive.drive_id}: {status} - {len(drive.sectors)} sectors used")
        
        print(f"{'='*60}")
    
    def cleanup(self):
        """Cleanup resources"""
        self.rebuild_active = False
        if self.rebuild_thread and self.rebuild_thread.is_alive():
            self.rebuild_thread.join(timeout=2.0)
            
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