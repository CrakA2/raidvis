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
        self.sectors = {} # Maps physical sector ID to {"data": ..., "type": ...}
        self.next_physical_sector = 0 # Independent counter for physical sectors on this drive
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
    
    def write_sector(self, data: str, block_type: str = "DATA"):
        """Write data to the next available physical sector on this drive."""
        if not self.is_active:
            raise Exception(f"Drive {self.drive_id} is not active")
        
        current_physical_sector = self.next_physical_sector
        self.sectors[current_physical_sector] = {"data": data, "type": block_type}
        self.metadata["used_sectors"] = len(self.sectors)
        self.next_physical_sector += 1 # Increment for the next write
        
        self._update_file()
        logger.log(f"Drive {self.drive_id}: Written '{data}' to physical sector {current_physical_sector} as {block_type}")
        return current_physical_sector # Return the physical sector it was written to
    
    def write_to_specific_sector(self, sector_num: int, data: str, block_type: str = "DATA"):
        """Allows writing to a specific physical sector, used for rebuilds."""
        if not self.is_active:
            raise Exception(f"Drive {self.drive_id} is not active")
        
        self.sectors[sector_num] = {"data": data, "type": block_type}
        self.metadata["used_sectors"] = len(self.sectors)
        # We don't increment next_physical_sector here, as this is a targeted write.
        self._update_file()
        logger.log(f"Drive {self.drive_id}: (Rebuild) Written '{data}' to physical sector {sector_num} as {block_type}")

    def read_sector(self, sector: int) -> Optional[str]:
        """Read data from a specific physical sector"""
        if not self.is_active:
            # If a drive is marked inactive, we cannot read from it.
            logger.log(f"Attempted read from failed drive {self.drive_id}", "ERROR")
            return None
        
        # Check if the drive file exists before attempting to read.
        # This is a basic sanity check for manual file deletion.
        if not os.path.exists(self.file_path):
            logger.log(f"Drive {self.drive_id} file not found. Marking as failed.", "ERROR")
            self.mark_failed() # Mark as failed if file is gone
            return None

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
                f.write(f"Used Sectors: {self.metadata['used_sectors']}\n")
                f.write(f"Next Physical Sector: {self.next_physical_sector}\n\n")
                f.write("BLOCK DIAGRAM:\n")
                f.write("+--------+--------+--------+--------+\n")
                f.write("| Sector | Block  | Type   | Data   |\n")
                f.write("+--------+--------+--------+--------+\n")
                
                for sector_num in sorted(self.sectors.keys()):
                    sector_data = self.sectors[sector_num]
                    data_preview = sector_data["data"][:8] if len(sector_data["data"]) > 8 else sector_data["data"]
                    # We'll use a placeholder for "Block" here as the physical drive
                    # doesn't inherently know the logical block grouping without more tracking.
                    f.write(f"|   {sector_num:2d}   | LBlock-- | {sector_data['type']:6s} | {data_preview:6s} |\n")
                
                f.write("+--------+--------+--------+--------+\n")
        except FileNotFoundError: # Handle case where file is deleted externally
            if self.is_active: # Only log if it wasn't already marked failed
                logger.log(f"Drive {self.drive_id} file not found during update. Marking as failed.", "ERROR")
                self.mark_failed()
        except Exception as e:
            logger.log(f"Error updating drive file {self.file_path}: {e}", "ERROR")
            
# TODO: Implement RAID levels 0, 1, and 5 with their specific logic
# Task completed 

class RAIDArray:
    """Main RAID array implementation"""
    
    def __init__(self, raid_level: int):
        self.raid_level = raid_level
        self.drives = []
        self.folder_path = f"raid_{raid_level}"
        self.rebuild_active = False
        self.rebuild_thread = None
        self.current_logical_block_index = 0 # Represents the logical block number being written
        self.logical_to_physical_map: Dict[int, Dict[int, int]] = {} # LBA -> {drive_id: physical_sector_num}
        
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
        # TODO add and remove before rebuilt can result in logic errors.
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
                    self.start_rebuild(failed_drive_id=drive_id, replacement_drive_id=replacement_id)
        else:
            logger.log(f"Cannot remove drive {drive_id}: drive not found or already inactive", "ERROR")
    
    def write_data(self, data: str):
        """Write data to the RAID array based on RAID level"""
        logger.log(f"Writing data: '{data}' to RAID-{self.raid_level}")
        
        current_lba = self.current_logical_block_index
        self.logical_to_physical_map[current_lba] = {}
        
        try:
            if self.raid_level == 0:
                self._write_raid0(data, current_lba)
            elif self.raid_level == 1:
                self._write_raid1(data, current_lba)
            elif self.raid_level == 5:
                self._write_raid5(data, current_lba)
            
            # Increment the logical block counter after a complete write operation
            self.current_logical_block_index += 1
            logger.log(f"RAID-{self.raid_level} write operation completed for logical block {current_lba}")
        except Exception as e:
            logger.log(f"RAID-{self.raid_level} write failed for logical block {current_lba}: {e}", "ERROR")
            # If a write fails, you might want to consider the block invalid or the RAID in a worse state.
            # For this demo, we'll just log the error and not increment the LBA if it failed.
            if current_lba in self.logical_to_physical_map:
                del self.logical_to_physical_map[current_lba] # Clean up failed LBA

    
    def _write_raid0(self, data: str, lba: int):
        """RAID-0 striping implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if not active_drives:
            logger.log("No active drives available", "ERROR")
            raise Exception("No active drives for RAID-0 write")
        
        # Split data into blocks (characters for this demo) and stripe across drives
        for i, char in enumerate(data):
            drive_index = i % len(active_drives)
            drive_to_write = active_drives[drive_index]
            
            try:
                physical_sector = drive_to_write.write_sector(char, "DATA")
                self.logical_to_physical_map[lba][drive_to_write.drive_id] = physical_sector
            except Exception as e:
                logger.log(f"Error writing to Drive {drive_to_write.drive_id}: {e}", "ERROR")
                # Mark drive failed if write fails
                drive_to_write.mark_failed()
                raise # Re-raise to indicate overall write failure
            
            time.sleep(0.2)  # Slow down for demonstration
    
    def _write_raid1(self, data: str, lba: int):
        """RAID-1 mirroring implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if not active_drives:
            logger.log("No active drives available", "ERROR")
            raise Exception("No active drives for RAID-1 write")
        
        # Write same data to all drives
        for char in data:
            for drive in active_drives:
                try:
                    physical_sector = drive.write_sector(char, "DATA")
                    self.logical_to_physical_map[lba][drive.drive_id] = physical_sector
                except Exception as e:
                    logger.log(f"Error writing to Drive {drive.drive_id}: {e}", "ERROR")
                    drive.mark_failed() # Mark drive failed if write fails
                    # In RAID-1, failure of one drive doesn't necessarily fail the whole write if others succeed.
                    # But for simplicity in demo, we'll indicate if any single write fails.
                    raise # Re-raise to indicate overall write failure
                time.sleep(0.1)  # Slow down for demonstration
    
    def _write_raid5(self, data: str, lba: int):
        """RAID-5 striping with parity implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if len(active_drives) < 3:
            logger.log("RAID-5 requires at least 3 active drives", "ERROR")
            raise Exception("Not enough active drives for RAID-5 write")
        
        # Calculate parity drive position (rotating)
        parity_drive_index = lba % len(active_drives)
        
        # Data drives exclude the parity drive for this stripe
        data_drives_for_stripe = [active_drives[i] for i in range(len(active_drives)) if i != parity_drive_index]
        parity_data_chars = ""
        
        # Write data blocks to non-parity drives
        for i, char in enumerate(data):
            if i < len(data_drives_for_stripe):
                drive_to_write = data_drives_for_stripe[i]
                try:
                    physical_sector = drive_to_write.write_sector(char, "DATA")
                    self.logical_to_physical_map[lba][drive_to_write.drive_id] = physical_sector
                    parity_data_chars += char
                except Exception as e:
                    logger.log(f"Error writing data to Drive {drive_to_write.drive_id}: {e}", "ERROR")
                    drive_to_write.mark_failed()
                    raise
                time.sleep(0.2)
        
        # Calculate and write parity
        time.sleep(0.5)  # Simulate parity calculation time
        parity_char = self._calculate_parity(parity_data_chars)
        
        parity_drive = active_drives[parity_drive_index]
        try:
            physical_sector = parity_drive.write_sector(parity_char, "PARITY")
            self.logical_to_physical_map[lba][parity_drive.drive_id] = physical_sector
            logger.log(f"Parity '{parity_char}' calculated and stored on drive {parity_drive.drive_id}")
        except Exception as e:
            logger.log(f"Error writing parity to Drive {parity_drive.drive_id}: {e}", "ERROR")
            parity_drive.mark_failed()
            raise
    
    def _calculate_parity(self, data: str) -> str:
        """Simple XOR parity calculation for demonstration"""
        if not data:
            return "0000"  # Default parity block
        
        # Pad data to block size if needed (for more realistic parity)
        # Assuming block_size represents the unit for parity calculation,
        # here we'll keep it simple for character-based XOR.
        
        parity_val = 0
        for char in data:
            parity_val ^= ord(char)
        
        # Create readable parity representation
        # Using a fixed length for display
        parity_str = f"P{parity_val:03d}"[:4] # Truncate/pad to 4 characters
        return parity_str
    
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
            replacement_drive.is_active = True # New drive is now active
            replacement_drive.metadata["status"] = "rebuilding"
            
            logger.log("REBUILD: Starting drive rebuild process")
            
            # Iterate through all logical blocks that were written

            total_logical_blocks = self.current_logical_block_index
            if total_logical_blocks == 0:
                logger.log("REBUILD: No data blocks to rebuild.", "INFO")
                return

            for lba in range(total_logical_blocks):
                if not self.rebuild_active:
                    break
                
                # Simulate rebuild delay
                time.sleep(0.5)
                
                # Get the physical sector mapping for this logical block
                physical_sector_map_for_lba = self.logical_to_physical_map.get(lba, {})
                
                # Determine the physical sector on the *failed* drive for this LBA
                target_physical_sector_on_failed_drive = physical_sector_map_for_lba.get(failed_drive_id)
                
                if target_physical_sector_on_failed_drive is None:
                    # This logical block might not have touched the failed drive, or it was a partial write.
                    # Or it could be a control block, etc. Skip for now.
                    # For a simple demo, we only rebuild what was written.
                    logger.log(f"REBUILD: Logical block {lba} did not involve failed drive {failed_drive_id} (or data lost), skipping.", "INFO")
                    continue

                rebuilt_data = None
                
                if self.raid_level == 1:
                    # RAID-1: Copy from any active mirror
                    for drive in self.drives:
                        if drive.is_active and drive.drive_id != failed_drive_id:
                            # We need to know *which physical sector* on the mirror corresponds to this LBA
                            mirror_physical_sector = physical_sector_map_for_lba.get(drive.drive_id)
                            if mirror_physical_sector is not None:
                                data = drive.read_sector(mirror_physical_sector)
                                if data is not None:
                                    rebuilt_data = data
                                    break # Found data, no need to check other mirrors
                                else:
                                    logger.log(f"REBUILD WARN: Could not read data from drive {drive.drive_id} for LBA {lba} at physical sector {mirror_physical_sector}", "WARN")

                elif self.raid_level == 5:
                    # RAID-5: Reconstruct from remaining drives and parity

                    original_active_drives_count = len(self.drives) # Assuming initial configuration
                    if not self.drives[failed_drive_id].is_active: # Exclude the failed drive from this count if it was initially active
                         original_active_drives_count -= 1

                    parity_drive_index_for_lba = lba % len(self.drives) # Use total drives count for the modulus

                    data_blocks_for_reconstruct = []
                    parity_block_for_reconstruct = None
                    
                    for d_idx, drive in enumerate(self.drives):
                        if drive.drive_id == failed_drive_id or not drive.is_active:
                            continue # Skip the failed drive and any other inactive drives
                        
                        physical_sector_num = physical_sector_map_for_lba.get(drive.drive_id)
                        if physical_sector_num is None:
                            continue # This drive might not have had data for this LBA

                        data_from_drive = drive.read_sector(physical_sector_num)
                        if data_from_drive is not None:
                            if d_idx == parity_drive_index_for_lba:
                                parity_block_for_reconstruct = data_from_drive
                            else:
                                data_blocks_for_reconstruct.append(data_from_drive)
                        else:
                             logger.log(f"REBUILD WARN: Could not read data from drive {drive.drive_id} for LBA {lba} at physical sector {physical_sector_num}", "WARN")
                    
                    if len(data_blocks_for_reconstruct) > 0:
                        # Simple XOR reconstruction based on character values
                        reconstructed_val = 0
                        for char_data in data_blocks_for_reconstruct:
                            reconstructed_val ^= ord(char_data)
                        
                        if parity_block_for_reconstruct:
                            # If parity exists, XOR it back in to find the missing data
                            parity_val_from_str = int(parity_block_for_reconstruct[1:]) # Extract numeric part
                            reconstructed_val ^= parity_val_from_str
                        
                        rebuilt_data = chr(reconstructed_val % 128) # Convert back to char (limited ASCII)
                    else:
                        logger.log(f"REBUILD WARN: Not enough data blocks to reconstruct LBA {lba} for RAID-5.", "WARN")
                        rebuilt_data = "???" # Placeholder if reconstruction fails
                
                if rebuilt_data is not None:
                    replacement_drive.write_to_specific_sector(target_physical_sector_on_failed_drive, rebuilt_data, "REBUILT")
                    # Update map for the new drive
                    self.logical_to_physical_map[lba][replacement_drive_id] = target_physical_sector_on_failed_drive
                else:
                    logger.log(f"REBUILD ERROR: Failed to reconstruct data for LBA {lba} on failed drive {failed_drive_id}", "ERROR")
                    replacement_drive.write_to_specific_sector(target_physical_sector_on_failed_drive, "ERROR", "REBUILD-FAIL")


                progress = ((lba + 1) / total_logical_blocks) * 100
                logger.log(f"REBUILD: Progress {progress:.1f}% - Logical Block {lba}")
            
            replacement_drive.metadata["status"] = "active" # Mark as active after rebuild
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
        print(f"Current Logical Block Index: {self.current_logical_block_index}")
        print(f"Rebuild Active: {'Yes' if self.rebuild_active else 'No'}")
        print()
        
        for drive in self.drives:
            status = "ACTIVE" if drive.is_active else "FAILED"
            print(f"Drive {drive.drive_id}: {status} - {drive.next_physical_sector} physical sectors written")
        
        print(f"\nLogical Block to Physical Sector Mapping:")
        if not self.logical_to_physical_map:
            print("  No logical blocks written yet.")
        else:
            for lba, drive_sector_map in sorted(self.logical_to_physical_map.items()):
                print(f"  LBA {lba}: {drive_sector_map}")
        
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