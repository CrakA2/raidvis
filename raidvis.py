import os
import shutil
import threading
import time
import queue
import datetime
from typing import List, Dict, Optional
import json
from typing import List, Dict, Optional, Any
import random

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
        with open(self.log_file, "a") as f:
            while logging_active:
                try:
                    message = log_queue.get(timeout=0.1)
                    print(message)
                    f.write("\n" +message + "\n")
                    f.flush()
                except queue.Empty:
                    continue
                
                
#------------------------------------------
# Drive Class and definition
# ---------------------------------------------------------------------
class Drive:
    """Represents a single drive in the RAID array"""

    def __init__(
        self, drive_id: int, folder_path: str, signature: Optional[str] = None
    ):  # Added signature param
        self.drive_id = drive_id
        self.folder_path = folder_path
        self.file_path = os.path.join(folder_path, f"disk_{drive_id}")
        self.is_active = True
        self.sectors: Dict[int, Dict[str, Any]] = {}
        self.next_physical_sector = 0

        # Drive-specific signature for rebuild/re-add checks
        self.signature = (
            signature
            if signature
            else f"DRV-{drive_id}-{random.randint(10000,99999)}-{datetime.datetime.now().timestamp()}"
        )

        self.metadata = {
            "drive_id": drive_id,
            "creation_time": datetime.datetime.now().isoformat(),
            "status": "active",
            "total_sectors": 0,
            "used_sectors": 0,
            "signature": self.signature,
        }
        self.create_drive_file()

    def create_drive_file(self):
        """Initialize the drive file with header and metadata"""
        try:
            with open(self.file_path, "w") as f:
                f.write(f"{'='*50}\n")
                f.write(f"RAID DRIVE {self.drive_id} - DEMONSTRATION FILE\n")
                f.write(f"{'='*50}\n\n")
                f.write("METADATA:\n")
                f.write(f"Drive ID: {self.drive_id}\n")
                f.write(f"Status: {self.metadata['status']}\n")
                f.write(f"Created: {self.metadata['creation_time']}\n")
                f.write(f"Signature: {self.signature}\n")
                f.write(f"Part of RAID: [Will be updated]\n")
                f.write(f"Position in RAID: [Will be updated]\n")
                f.write(f"Rebuild Rate: N/A\n\n")
                f.write("BLOCK DIAGRAM:\n")
                f.write("+--------+--------+--------+--------+\n")
                f.write("| Sector | LBlock | Type   | Data   |\n")
                f.write("+--------+--------+--------+--------+\n")
        except Exception as e:
            logger.log(f"Error creating drive file {self.file_path}: {e}", "ERROR")

    def write_sector(
        self, data: str, block_type: str = "DATA", lba: Optional[int] = None
    ):
        """Write data to the next available physical sector on this drive, associating it with an LBA."""
        if not self.is_active:
            raise Exception(f"Drive {self.drive_id} is not active")

        current_physical_sector = self.next_physical_sector
        self.sectors[current_physical_sector] = {
            "data": data,
            "type": block_type,
            "lba": lba,
        }
        self.metadata["used_sectors"] = len(self.sectors)
        self.next_physical_sector += 1

        self._update_file()
        logger.log(
            f"Drive {self.drive_id}: Written '{data}' to physical sector {current_physical_sector} (LBA: {lba if lba is not None else 'N/A'}) as {block_type}"
        )
        return current_physical_sector

    def write_to_specific_sector(
        self, sector_num: int, data: str, block_type: str = "DATA", lba: Optional[int] = None
    ):
        """Allows writing to a specific physical sector, used for rebuilds, associating it with an LBA."""
        if not self.is_active:
            raise Exception(f"Drive {self.drive_id} is not active")

        self.sectors[sector_num] = {"data": data, "type": block_type, "lba": lba}
        self.metadata["used_sectors"] = len(self.sectors)
        self._update_file()
        logger.log(
            f"Drive {self.drive_id}: (Rebuild) Written '{data}' to physical sector {sector_num} (LBA: {lba if lba is not None else 'N/A'}) as {block_type}"
        )

    def read_sector(self, sector: int) -> Optional[str]:
        """Read data from a specific physical sector"""
        if not self.is_active:
            logger.log(f"Attempted read from failed drive {self.drive_id}", "ERROR")
            return None

        if not os.path.exists(self.file_path):
            logger.log(
                f"Drive {self.drive_id} file not found. Marking as failed.", "ERROR"
            )
            self.mark_failed()
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
            with open(self.file_path, "w") as f:
                f.write(f"{'='*50}\n")
                f.write(f"RAID DRIVE {self.drive_id} - DEMONSTRATION FILE\n")
                f.write(f"{'='*50}\n\n")
                f.write("METADATA:\n")
                f.write(f"Drive ID: {self.drive_id}\n")
                f.write(f"Status: {self.metadata['status']}\n")
                f.write(f"Created: {self.metadata['creation_time']}\n")
                f.write(f"Signature: {self.signature}\n")
                f.write(f"Used Sectors: {self.metadata['used_sectors']}\n")
                f.write(f"Next Physical Sector: {self.next_physical_sector}\n\n")
                f.write("BLOCK DIAGRAM:\n")
                f.write("+--------+--------+--------+--------+\n")
                f.write("| Sector | LBlock | Type   | Data   |\n")
                f.write("+--------+--------+--------+--------+\n")

                for sector_num in sorted(self.sectors.keys()):
                    sector_data = self.sectors[sector_num]
                    data_preview = (
                        sector_data["data"][:8]
                        if len(sector_data["data"]) > 8
                        else sector_data["data"]
                    )
                    lba_display = (
                        f"{sector_data['lba']:6d}"
                        if sector_data["lba"] is not None
                        else "   N/A"
                    )
                    f.write(
                        f"|   {sector_num:2d}   | {lba_display} | {sector_data['type']:6s} | {data_preview:6s} |\n"
                    )

                f.write("+--------+--------+--------+--------+\n")
        except FileNotFoundError:
            if self.is_active:
                logger.log(
                    f"Drive {self.drive_id} file not found during update. Marking as failed.",
                    "ERROR",
                )
                self.mark_failed()
        except Exception as e:
            logger.log(f"Error updating drive file {self.file_path}: {e}", "ERROR")

# TODO: Implement RAID levels 0, 1, and 5 with their specific logic
# Task completed 

class RAIDArray:
    """Main RAID array implementation"""

    def __init__(self, raid_level: int):
        self.raid_level = raid_level
        self.drives: List[Drive] = []  # Ensure drives list is correctly typed
        self.folder_path = f"raid_{raid_level}"
        self.rebuild_active = False
        self.rebuild_thread = None
        self.current_logical_block_index = 0
        self.logical_to_physical_map: Dict[int, Dict[int, int]] = {}

        self.raid_signature = f"RAID-{raid_level}-{datetime.datetime.now().timestamp()}"
        self.config_file = os.path.join(self.folder_path, "raid_config.json")

        self.raid_configs = {
            0: {"min_drives": 2, "name": "RAID-0 (Striping)", "fault_tolerance": 0},
            1: {"min_drives": 2, "name": "RAID-1 (Mirroring)", "fault_tolerance": 1},
            5: {
                "min_drives": 3,
                "name": "RAID-5 (Striping with Parity)",
                "fault_tolerance": 1,
            },
            6: {
                "min_drives": 4,
                "name": "RAID-6 (Striping with Dual Parity)",
                "fault_tolerance": 2,
            },
            10: {
                "min_drives": 4,
                "name": "RAID-10 (Mirrored Stripes)",
                "fault_tolerance": 2,
            },  # Min 2 mirrors of 2-drive RAID-0 arrays
            50: {
                "min_drives": 6,
                "name": "RAID-50 (Striped RAID-5)",
                "fault_tolerance": 2,
            },
            60: {
                "min_drives": 8,
                "name": "RAID-60 (Striped RAID-6)",
                "fault_tolerance": 4,
            },
        }

        # Initialize, but don't automatically clear if loading from main

    def initialize_raid_structure(self, clear_existing: bool = True):  # Renamed for clarity
        """Initializes the RAID array, including folder, config, and drives."""
        logger.log(
            f"Initializing {self.raid_configs[self.raid_level]['name']} structure (clear_existing={clear_existing})"
        )

        if os.path.exists(self.folder_path):
            if clear_existing:
                logger.log(f"Clearing existing RAID folder: {self.folder_path}")
                shutil.rmtree(self.folder_path)
            else:
                # If not clearing, don't create folder again. _load_config will handle it.
                pass

        if clear_existing:
            os.makedirs(self.folder_path, exist_ok=True)  # Ensure folder exists if cleared

            # Create minimum required drives *from scratch*
            self.drives = []  # Crucial: reset the list for fresh initialization
            min_drives = self.raid_configs[self.raid_level]["min_drives"]
            for i in range(min_drives):
                self.add_drive()  # Use add_drive to assign new, sequential IDs

            self.current_logical_block_index = 0
            self.logical_to_physical_map = {}
            self.raid_signature = f"RAID-{self.raid_level}-{datetime.datetime.now().timestamp()}"  # New signature for new RAID
            self._save_config()  # Save initial configuration
            logger.log(
                f"RAID-{self.raid_level} initialized fresh with {len(self.drives)} drives"
            )
        else:
            # If not clearing, attempt to load config. If it fails, then create a new one.
            if not self._load_config():
                logger.log("Failed to load RAID configuration. Creating a new one.", "WARN")
                # Fallback to clear and create new if load fails
                self.initialize_raid_structure(clear_existing=True)
            else:
                logger.log(
                    f"RAID-{self.raid_level} configuration loaded with {len(self.drives)} drives."
                )

    def _save_config(self):
        """Saves the current RAID configuration to a JSON file."""
        config_data = {
            "raid_level": self.raid_level,
            "raid_signature": self.raid_signature,
            "current_logical_block_index": self.current_logical_block_index,
            "logical_to_physical_map": {
                str(lba): {str(d_id): p_sec for d_id, p_sec in d_map.items()}
                for lba, d_map in self.logical_to_physical_map.items()
            },
            "drives": [
                {
                    "id": d.drive_id,
                    "signature": d.signature,
                    "is_active": d.is_active,
                    "status": d.metadata["status"],  # Save current status
                    "next_physical_sector": d.next_physical_sector,
                }
                for d in self.drives
            ],
        }
        try:
            with open(self.config_file, "w") as f:
                json.dump(config_data, f, indent=4)
            logger.log(f"RAID configuration saved to {self.config_file}")
        except Exception as e:
            logger.log(f"Error saving RAID configuration: {e}", "ERROR")

    def _load_config(self) -> bool:
        """Loads RAID configuration from a JSON file."""
        if not os.path.exists(self.config_file):
            logger.log(f"RAID config file not found: {self.config_file}", "INFO")
            return False

        try:
            with open(self.config_file, "r") as f:
                config_data = json.load(f)

            if config_data.get("raid_level") != self.raid_level:
                logger.log(
                    f"Mismatched RAID level in config file. Expected {self.raid_level}, got {config_data.get('raid_level')}",
                    "ERROR",
                )
                return False

            self.raid_signature = config_data.get("raid_signature", self.raid_signature)
            self.current_logical_block_index = config_data.get(
                "current_logical_block_index", 0
            )

            loaded_map = config_data.get("logical_to_physical_map", {})
            self.logical_to_physical_map = {
                int(lba): {int(d_id): p_sec for d_id, p_sec in d_map.items()}
                for lba, d_map in loaded_map.items()
            }

            self.drives = []  # Reset drives list before loading
            for drive_info in config_data.get("drives", []):
                drive_id = drive_info["id"]
                drive_signature = drive_info["signature"]
                drive_status = drive_info.get("status", "unknown")

                drive_file_path = os.path.join(self.folder_path, f"disk_{drive_id}")

                # Check actual physical drive file existence and signature consistency
                if not os.path.exists(drive_file_path):
                    logger.log(
                        f"Drive file {drive_file_path} missing for drive {drive_id}. Marking as failed.",
                        "ERROR",
                    )
                    new_drive = Drive(
                        drive_id, self.folder_path, signature=drive_signature
                    )  # Re-create with original signature
                    new_drive.is_active = False
                    new_drive.metadata["status"] = "failed"
                    new_drive.next_physical_sector = drive_info.get(
                        "next_physical_sector", 0
                    )
                    new_drive.sectors = {}  # No data loaded if file is missing
                else:
                    # Assume file has valid metadata and sectors as per _update_file (no direct parsing here)
                    # When a Drive object is initialized, it creates its own file.
                    # We need to override with *loaded* metadata.
                    new_drive = Drive(
                        drive_id, self.folder_path, signature=drive_signature
                    )

                    # Verify signature if drive was previously active
                    # This is a conceptual check; in a real system, you'd read the drive's own signature.
                    if drive_status != "failed" and new_drive.signature != drive_signature:
                        logger.log(
                            f"Drive {drive_id} signature mismatch. Expected {drive_signature}, found {new_drive.signature}. Marking as failed.",
                            "ERROR",
                        )
                        new_drive.is_active = False
                        new_drive.metadata["status"] = "failed_signature_mismatch"
                    else:
                        new_drive.is_active = drive_info.get("is_active", False)
                        new_drive.metadata["status"] = drive_status

                    new_drive.next_physical_sector = drive_info.get(
                        "next_physical_sector", 0
                    )

                self.drives.append(new_drive)

            # Important: Ensure `self.drives` is sorted by `drive_id` to maintain consistent indexing
            self.drives.sort(key=lambda d: d.drive_id)
            return True
        except Exception as e:
            logger.log(f"Error loading RAID configuration: {e}", "ERROR")
            return False

    def add_drive(self):
        """Add a new drive to the RAID array, assigning it the next available ID."""
        # Find the next sequential drive_id
        if not self.drives:
            new_drive_id = 0
        else:
            # Ensures unique and sequential IDs, even if some drives were removed from the middle.
            existing_ids = sorted([d.drive_id for d in self.drives])
            new_drive_id = 0
            for i in existing_ids:
                if i == new_drive_id:
                    new_drive_id += 1
                else:
                    break  # Found a gap

        new_drive = Drive(new_drive_id, self.folder_path)
        self.drives.append(new_drive)
        self.drives.sort(key=lambda d: d.drive_id)  # Keep sorted
        self._save_config()
        logger.log(f"Added drive {new_drive_id} to RAID-{self.raid_level}")
        return new_drive_id

    def remove_drive(self, drive_id_to_fail: int):  # Renamed parameter for clarity
        """Remove a drive from the RAID array (simulate failure)."""
        if (
            not (0 <= drive_id_to_fail < len(self.drives))
            or self.drives[drive_id_to_fail].drive_id != drive_id_to_fail
        ):
            logger.log(
                f"Drive ID {drive_id_to_fail} not found or index mismatch. Please use a valid ID from display_status.",
                "ERROR",
            )
            return

        failed_drive_obj = self.drives[drive_id_to_fail]

        if not failed_drive_obj.is_active:
            logger.log(f"Drive {drive_id_to_fail} is already inactive/failed.", "WARN")
            return  # Already failed, don't re-prompt for rebuild

        failed_drive_obj.mark_failed()
        self._save_config()

        active_drives_count = sum(1 for d in self.drives if d.is_active)
        fault_tolerance = self.raid_configs[self.raid_level]["fault_tolerance"]

        # Check against initial number of drives, as fault tolerance is based on array size
        if active_drives_count < (len(self.drives) - fault_tolerance):
            logger.log("RAID FAILURE: Not enough drives to maintain data integrity!", "ERROR")
            print("\n!!! CRITICAL RAID FAILURE !!! Data may be lost.")
            if self.raid_level == 0:
                print(
                    "RAID-0 has no fault tolerance. Data on logical drive is permanently failed."
                )
                for lba_map in self.logical_to_physical_map.values():
                    if drive_id_to_fail in lba_map:
                        lba_map[
                            drive_id_to_fail
                        ] = -1  # Mark as permanently failed block
            return  # Exit, no recovery options for critical failure

        else:
            logger.log(f"RAID-{self.raid_level} operating in degraded mode", "WARN")
            print(f"\nRAID is operating in degraded mode. Failed drive: {drive_id_to_fail}")

            print("1. Re-add existing drive (if it was temporarily removed and is now back)")
            print("2. Add new replacement drive and start rebuild")
            print("3. Do nothing for now")

            choice = input("Enter your choice (1-3): ").strip()

            if choice == "1":
                # Re-add drive logic: check if the *same physical drive* (by signature) is being returned
                # This assumes the drive file was never physically deleted/overwritten externally.
                if not failed_drive_obj.is_active and os.path.exists(
                    failed_drive_obj.file_path
                ):
                    # For RAID-0, a failed drive means permanent data loss.
                    # We can't "re-add" with original data integrity unless it's a new, empty drive.
                    # If the logical block was already marked as permanently failed, we don't try to re-add its data.
                    is_consistent = True
                    if self.raid_level == 0:
                        for lba in range(self.current_logical_block_index):
                            lba_map = self.logical_to_physical_map.get(lba, {})
                            if lba_map.get(drive_id_to_fail) == -1:  # Permanently failed
                                is_consistent = False
                                break

                    if is_consistent and failed_drive_obj.metadata.get("signature") == failed_drive_obj.signature:
                        # For RAID-1, 5, 6, 10, 50, 60, if signature matches and file exists, assume it can be re-added
                        # We still need to check data consistency conceptually.
                        # For a true system, a re-added drive would need to be re-synced from mirrors/parity.
                        # For this demo, if signature matches, we allow it.
                        failed_drive_obj.is_active = True
                        failed_drive_obj.metadata["status"] = "active"
                        failed_drive_obj._update_file()
                        logger.log(
                            f"Drive {drive_id_to_fail} re-added. Signature match: {failed_drive_obj.signature}",
                            "INFO",
                        )
                        self._save_config()
                        # If RAID 0 and it was marked failed, this re-add cannot recover previous data.
                        # For other RAIDs, assume resync happens implicitly.
                    elif self.raid_level == 0 and not is_consistent:
                        logger.log(
                            f"Cannot re-add drive {drive_id_to_fail} for RAID-0. Logical block data permanently failed.",
                            "ERROR",
                        )
                        failed_drive_obj.metadata["status"] = "permanently_failed"
                        failed_drive_obj._update_file()
                    else:
                        logger.log(
                            f"Cannot re-add drive {drive_id_to_fail}. Signature mismatch or other issue.",
                            "ERROR",
                        )
                else:
                    logger.log(
                        f"Cannot re-add drive {drive_id_to_fail}. Drive file missing or already active.",
                        "ERROR",
                    )

            elif choice == "2":
                replacement_id = self.add_drive()
                self.start_rebuild(
                    failed_logical_drive_position=drive_id_to_fail,
                    replacement_drive_id=replacement_id,
                )

    def write_data(self, data: str):
        """Write data to the RAID array based on RAID level"""
        logger.log(f"Writing data: '{data}' to RAID-{self.raid_level}")

        # Check if RAID is in a state that prevents writes
        active_drives_count = sum(1 for d in self.drives if d.is_active)
        fault_tolerance = self.raid_configs[self.raid_level]["fault_tolerance"]
        min_drives_for_write = (
            len(self.drives) - fault_tolerance
        )  # For RAID 0, this is len(drives)

        if self.raid_level == 0 and active_drives_count < len(self.drives):
            logger.log(
                "RAID-0 cannot write data with failed drives (no fault tolerance).", "ERROR"
            )
            print("Cannot write: RAID-0 has no fault tolerance and drives have failed.")
            return
        elif self.raid_level in [1, 5, 6, 10, 50, 60] and active_drives_count < min_drives_for_write:
            logger.log(
                f"RAID-{self.raid_level} cannot write data due to excessive drive failures.",
                "ERROR",
            )
            print(
                f"Cannot write: RAID-{self.raid_level} has too many failed drives. Needs at least {min_drives_for_write} active drives."
            )
            return

        current_lba = self.current_logical_block_index
        self.logical_to_physical_map[current_lba] = {}

        try:
            if self.raid_level == 0:
                self._write_raid0(data, current_lba)
            elif self.raid_level == 1:
                self._write_raid1(data, current_lba)
            elif self.raid_level == 5:
                self._write_raid5(data, current_lba)
            elif self.raid_level == 6:
                self._write_raid6(data, current_lba)
            elif self.raid_level == 10:
                self._write_raid10(data, current_lba)
            elif self.raid_level == 50:
                self._write_raid50(data, current_lba)
            elif self.raid_level == 60:
                self._write_raid60(data, current_lba)

            self.current_logical_block_index += 1
            self._save_config()
            logger.log(f"RAID-{self.raid_level} write operation completed for logical block {current_lba}")
        except Exception as e:
            logger.log(
                f"RAID-{self.raid_level} write failed for logical block {current_lba}: {e}",
                "ERROR",
            )
            if current_lba in self.logical_to_physical_map:
                del self.logical_to_physical_map[current_lba]

    def _write_raid0(self, data: str, lba: int):
        """RAID-0 striping implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if not active_drives:
            logger.log("No active drives available", "ERROR")
            raise Exception("No active drives for RAID-0 write")

        # In RAID-0, if a drive fails, data is lost. We cannot write if any drive is failed.
        if len(active_drives) < len(self.drives):
            raise Exception("RAID-0 cannot write data with failed drives (no fault tolerance)")

        for i, char in enumerate(data):
            # This logic distributes characters across the *currently active* drives
            # using the modulo of their index within the active_drives list.
            drive_to_write = active_drives[i % len(active_drives)]

            try:
                physical_sector = drive_to_write.write_sector(char, "DATA", lba)
                self.logical_to_physical_map[lba][drive_to_write.drive_id] = physical_sector
            except Exception as e:
                logger.log(f"Error writing to Drive {drive_to_write.drive_id}: {e}", "ERROR")
                drive_to_write.mark_failed()
                # Mark this logical block as permanently failed for this drive if it fails mid-write
                self.logical_to_physical_map[lba][drive_to_write.drive_id] = -1
                raise  # Re-raise to stop the entire write
            time.sleep(0.2)

    def _write_raid1(self, data: str, lba: int):
        """RAID-1 mirroring implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if not active_drives:
            logger.log("No active drives available", "ERROR")
            raise Exception("No active drives for RAID-1 write")

        # In RAID-1, write to all active mirrors. If one fails, others should still work.
        # But if all fail, it's critical. We need at least one active drive for any write.
        if not active_drives:
            raise Exception("No active drives for RAID-1 write, cannot perform IO.")

        successful_writes = 0
        for char in data:
            for drive in self.drives:  # Iterate over all drives, active or not
                if drive.is_active:
                    try:
                        physical_sector = drive.write_sector(char, "DATA", lba)
                        self.logical_to_physical_map[lba][drive.drive_id] = physical_sector
                        successful_writes += 1
                    except Exception as e:
                        logger.log(f"Error writing to Drive {drive.drive_id}: {e}", "ERROR")
                        drive.mark_failed()
                time.sleep(0.1)
            # Check if sufficient writes occurred for fault tolerance
            if successful_writes < len(self.drives) - self.raid_configs[self.raid_level]['fault_tolerance']:
                raise Exception("Not enough drives successfully written for RAID-1 fault tolerance")

    def _write_raid5(self, data: str, lba: int):
        """RAID-5 striping with parity implementation"""
        active_drives = [d for d in self.drives if d.is_active]
        if len(active_drives) < 3:
            logger.log("RAID-5 requires at least 3 active drives", "ERROR")
            raise Exception("Not enough active drives for RAID-5 write")

        parity_drive_active_index = lba % len(active_drives)
        parity_drive = active_drives[parity_drive_active_index]

        data_drives_for_stripe = [
            d for i, d in enumerate(active_drives) if i != parity_drive_active_index
        ]
        parity_data_chars = ""

        # Ensure we have enough data drives to strip across
        if not data_drives_for_stripe:
            raise Exception("Not enough data drives for RAID-5 striping.")

        for i, char in enumerate(data):
            if i < len(data_drives_for_stripe):
                drive_to_write = data_drives_for_stripe[i]
                try:
                    physical_sector = drive_to_write.write_sector(char, "DATA", lba)
                    self.logical_to_physical_map[lba][drive_to_write.drive_id] = physical_sector
                    parity_data_chars += char
                except Exception as e:
                    logger.log(f"Error writing data to Drive {drive_to_write.drive_id}: {e}", "ERROR")
                    drive_to_write.mark_failed()
                    raise
                time.sleep(0.2)

        time.sleep(0.5)
        parity_char = self._calculate_parity(parity_data_chars)

        try:
            physical_sector = parity_drive.write_sector(parity_char, "PARITY", lba)
            self.logical_to_physical_map[lba][parity_drive.drive_id] = physical_sector
            logger.log(f"Parity '{parity_char}' calculated and stored on drive {parity_drive.drive_id}")
        except Exception as e:
            logger.log(f"Error writing parity to Drive {parity_drive.drive_id}: {e}", "ERROR")
            parity_drive.mark_failed()
            raise

    def _write_raid6(self, data: str, lba: int):
        """RAID-6 striping with dual parity implementation (P and Q parity)"""
        active_drives = [d for d in self.drives if d.is_active]
        if len(active_drives) < 4:
            logger.log("RAID-6 requires at least 4 active drives", "ERROR")
            raise Exception("Not enough active drives for RAID-6 write")

        # Two parity drives are used
        parity_drive_1_idx = lba % len(active_drives)
        parity_drive_2_idx = (lba + 1) % len(active_drives)
        # Ensure parity drives are distinct
        if parity_drive_1_idx == parity_drive_2_idx:
            parity_drive_2_idx = (parity_drive_2_idx + 1) % len(active_drives)

        parity_drive_1 = active_drives[parity_drive_1_idx]
        parity_drive_2 = active_drives[parity_drive_2_idx]

        data_drives_for_stripe = [
            d
            for i, d in enumerate(active_drives)
            if i not in [parity_drive_1_idx, parity_drive_2_idx]
        ]
        if not data_drives_for_stripe:
            raise Exception("Not enough data drives for RAID-6 striping.")

        data_chars_for_p = ""
        data_values_for_q = []  # For GF(2^8) Reed-Solomon, for simplicity, use XOR with position weighting

        for i, char in enumerate(data):
            if i < len(data_drives_for_stripe):
                drive_to_write = data_drives_for_stripe[i]
                try:
                    physical_sector = drive_to_write.write_sector(char, "DATA", lba)
                    self.logical_to_physical_map[lba][drive_to_write.drive_id] = physical_sector
                    data_chars_for_p += char
                    data_values_for_q.append(
                        ord(char)
                    )  # Store ordinal for Q parity calculation
                except Exception as e:
                    logger.log(f"Error writing data to Drive {drive_to_write.drive_id}: {e}", "ERROR")
                    drive_to_write.mark_failed()
                    raise
                time.sleep(0.2)

        time.sleep(0.5)

        # Calculate P parity (simple XOR)
        p_parity_char = self._calculate_parity(data_chars_for_p)

        # Calculate Q parity (simplified for demo, typically Reed-Solomon)
        q_parity_val = 0
        for i, val in enumerate(data_values_for_q):
            q_parity_val ^= (
                val << i
            )  # Simplified: XOR with a shift based on data drive index
        q_parity_char = f"Q{q_parity_val % 1000:03d}"[:4]

        # Write P parity
        try:
            physical_sector_p = parity_drive_1.write_sector(p_parity_char, "PARITY-P", lba)
            self.logical_to_physical_map[lba][parity_drive_1.drive_id] = physical_sector_p
            logger.log(
                f"P-Parity '{p_parity_char}' stored on drive {parity_drive_1.drive_id}"
            )
        except Exception as e:
            logger.log(f"Error writing P-parity to Drive {parity_drive_1.drive_id}: {e}", "ERROR")
            parity_drive_1.mark_failed()
            raise

        # Write Q parity
        try:
            physical_sector_q = parity_drive_2.write_sector(q_parity_char, "PARITY-Q", lba)
            self.logical_to_physical_map[lba][parity_drive_2.drive_id] = physical_sector_q
            logger.log(
                f"Q-Parity '{q_parity_char}' stored on drive {parity_drive_2.drive_id}"
            )
        except Exception as e:
            logger.log(f"Error writing Q-parity to Drive {parity_drive_2.drive_id}: {e}", "ERROR")
            parity_drive_2.mark_failed()
            raise

    def _write_raid10(self, data: str, lba: int):
        """RAID-10 (RAID 1+0) implementation: Mirrored Stripes"""
        # Requires at least 4 drives. Assumes drives are paired for mirroring, then striped.
        # Example: (D0,D1) mirror, (D2,D3) mirror. Then data striped across (D0, D2)
        if len(self.drives) < 4 or len(self.drives) % 2 != 0:
            logger.log("RAID-10 requires an even number of at least 4 drives.", "ERROR")
            raise Exception("Invalid number of drives for RAID-10.")

        # Group drives into mirrored pairs
        mirrored_pairs: List[List[Drive]] = []
        for i in range(0, len(self.drives), 2):
            pair_drives = [self.drives[i], self.drives[i + 1]]
            # Check if at least one drive in the pair is active for writing
            if not any(d.is_active for d in pair_drives):
                raise Exception(f"Mirrored pair {i}-{i+1} is completely failed, cannot write.")
            mirrored_pairs.append(pair_drives)

        # Stripe data across the *first active drive* of each mirrored pair
        # This is a simplified representation of how data blocks are sent to RAID-0 arrays (sub-arrays)
        # and then mirrored within those sub-arrays.
        for i, char in enumerate(data):
            target_pair_index = i % len(mirrored_pairs)
            target_pair = mirrored_pairs[target_pair_index]

            successful_writes = 0
            for drive in target_pair:
                if drive.is_active:
                    try:
                        physical_sector = drive.write_sector(char, "DATA", lba)
                        self.logical_to_physical_map[lba][drive.drive_id] = physical_sector
                        successful_writes += 1
                    except Exception as e:
                        logger.log(f"Error writing to Drive {drive.drive_id}: {e}", "ERROR")
                        drive.mark_failed()
                time.sleep(0.1)

            if successful_writes == 0:
                raise Exception(
                    f"Failed to write character '{char}' to any drive in target mirrored pair for LBA {lba}."
                )

        logger.log(f"RAID-10: Data '{data}' written to mirrored stripes for LBA {lba}")

    def _write_raid50(self, data: str, lba: int):
        """RAID-50 (RAID 5+0) implementation: Striped RAID-5 sub-arrays"""
        # Requires a minimum of 6 drives (two RAID-5 arrays of 3 drives each)
        min_drives_for_subarray = self.raid_configs[5]["min_drives"]
        if (
            len(self.drives) < self.raid_configs[50]["min_drives"]
            or len(self.drives) % min_drives_for_subarray != 0
        ):
            logger.log(
                f"RAID-50 requires drives in multiples of {min_drives_for_subarray} (e.g., 6, 9, 12...)",
                "ERROR",
            )
            raise Exception("Invalid number of drives for RAID-50.")

        num_subarrays = len(self.drives) // min_drives_for_subarray
        subarrays: List[List[Drive]] = []
        for i in range(num_subarrays):
            subarray_drives = self.drives[
                i
                * min_drives_for_subarray : (i + 1)
                * min_drives_for_subarray
            ]
            subarrays.append(subarray_drives)

        # Distribute data across subarrays, and each subarray handles its own RAID-5 logic
        # For simplicity, we'll strip data characters across the *logical* first drive of each subarray.
        # This is a conceptual write; actual RAID-50 writes are more complex involving LVM.
        data_to_distribute = list(data)
        data_per_subarray = (len(data_to_distribute) + num_subarrays - 1) // num_subarrays

        for i in range(num_subarrays):
            subarray = subarrays[i]
            subarray_data_chunk = "".join(
                data_to_distribute[i * data_per_subarray : (i + 1) * data_per_subarray]
            )

            if not subarray_data_chunk:
                continue

            # Simulate RAID-5 write within the subarray
            active_subarray_drives = [d for d in subarray if d.is_active]
            if len(active_subarray_drives) < min_drives_for_subarray - self.raid_configs[5]['fault_tolerance']:
                logger.log(
                    f"RAID-50 Subarray {i} failed due to too many drive failures. Cannot write data.",
                    "ERROR",
                )
                raise Exception(f"RAID-50 Subarray {i} failed, cannot write.")

            parity_drive_active_index = lba % len(active_subarray_drives)
            parity_drive = active_subarray_drives[parity_drive_active_index]

            data_drives_for_stripe = [
                d
                for idx, d in enumerate(active_subarray_drives)
                if idx != parity_drive_active_index
            ]

            subarray_parity_data_chars = ""
            for j, char in enumerate(subarray_data_chunk):
                if j < len(data_drives_for_stripe):
                    drive_to_write = data_drives_for_stripe[j]
                    try:
                        physical_sector = drive_to_write.write_sector(char, "DATA", lba)
                        # Map to the global drive_id
                        self.logical_to_physical_map[lba][drive_to_write.drive_id] = physical_sector
                        subarray_parity_data_chars += char
                    except Exception as e:
                        logger.log(
                            f"Error writing data to Drive {drive_to_write.drive_id} in subarray {i}: {e}",
                            "ERROR",
                        )
                        drive_to_write.mark_failed()
                        raise
                    time.sleep(0.1)

            parity_char = self._calculate_parity(subarray_parity_data_chars)
            try:
                physical_sector_parity = parity_drive.write_sector(parity_char, "PARITY", lba)
                self.logical_to_physical_map[lba][parity_drive.drive_id] = physical_sector_parity
                logger.log(
                    f"RAID-50 Subarray {i}: Parity '{parity_char}' stored on drive {parity_drive.drive_id}"
                )
            except Exception as e:
                logger.log(
                    f"Error writing parity to Drive {parity_drive.drive_id} in subarray {i}: {e}",
                    "ERROR",
                )
                parity_drive.mark_failed()
                raise
            time.sleep(0.2)

        logger.log(f"RAID-50: Data '{data}' written to striped RAID-5 subarrays for LBA {lba}")

    def _write_raid60(self, data: str, lba: int):
        """RAID-60 (RAID 6+0) implementation: Striped RAID-6 sub-arrays"""
        # Requires a minimum of 8 drives (two RAID-6 arrays of 4 drives each)
        min_drives_for_subarray = self.raid_configs[6]["min_drives"]
        if (
            len(self.drives) < self.raid_configs[60]["min_drives"]
            or len(self.drives) % min_drives_for_subarray != 0
        ):
            logger.log(
                f"RAID-60 requires drives in multiples of {min_drives_for_subarray} (e.g., 8, 12, 16...)",
                "ERROR",
            )
            raise Exception("Invalid number of drives for RAID-60.")

        num_subarrays = len(self.drives) // min_drives_for_subarray
        subarrays: List[List[Drive]] = []
        for i in range(num_subarrays):
            subarray_drives = self.drives[
                i
                * min_drives_for_subarray : (i + 1)
                * min_drives_for_subarray
            ]
            subarrays.append(subarray_drives)

        data_to_distribute = list(data)
        data_per_subarray = (len(data_to_distribute) + num_subarrays - 1) // num_subarrays

        for i in range(num_subarrays):
            subarray = subarrays[i]
            subarray_data_chunk = "".join(
                data_to_distribute[i * data_per_subarray : (i + 1) * data_per_subarray]
            )

            if not subarray_data_chunk:
                continue

            # Simulate RAID-6 write within the subarray
            active_subarray_drives = [d for d in subarray if d.is_active]
            if len(active_subarray_drives) < min_drives_for_subarray - self.raid_configs[6]['fault_tolerance']:
                logger.log(
                    f"RAID-60 Subarray {i} failed due to too many drive failures. Cannot write data.",
                    "ERROR",
                )
                raise Exception(f"RAID-60 Subarray {i} failed, cannot write.")

            # RAID-6 dual parity logic for the subarray
            parity_drive_1_idx = lba % len(active_subarray_drives)
            parity_drive_2_idx = (lba + 1) % len(active_subarray_drives)
            if parity_drive_1_idx == parity_drive_2_idx:
                parity_drive_2_idx = (parity_drive_2_idx + 1) % len(active_subarray_drives)

            parity_drive_1 = active_subarray_drives[parity_drive_1_idx]
            parity_drive_2 = active_subarray_drives[parity_drive_2_idx]

            data_drives_for_stripe = [
                d
                for idx, d in enumerate(active_subarray_drives)
                if idx not in [parity_drive_1_idx, parity_drive_2_idx]
            ]
            if not data_drives_for_stripe:
                raise Exception(
                    f"RAID-60 Subarray {i}: Not enough data drives for RAID-6 striping."
                )

            subarray_data_chars_for_p = ""
            subarray_data_values_for_q = []

            for j, char in enumerate(subarray_data_chunk):
                if j < len(data_drives_for_stripe):
                    drive_to_write = data_drives_for_stripe[j]
                    try:
                        physical_sector = drive_to_write.write_sector(char, "DATA", lba)
                        self.logical_to_physical_map[lba][drive_to_write.drive_id] = physical_sector
                        subarray_data_chars_for_p += char
                        subarray_data_values_for_q.append(ord(char))
                    except Exception as e:
                        logger.log(
                            f"Error writing data to Drive {drive_to_write.drive_id} in subarray {i}: {e}",
                            "ERROR",
                        )
                        drive_to_write.mark_failed()
                        raise
                    time.sleep(0.1)

            p_parity_char = self._calculate_parity(subarray_data_chars_for_p)
            q_parity_val = 0
            for k, val in enumerate(subarray_data_values_for_q):
                q_parity_val ^= (val << k)
            q_parity_char = f"Q{q_parity_val % 1000:03d}"[:4]

            try:
                physical_sector_p = parity_drive_1.write_sector(p_parity_char, "PARITY-P", lba)
                self.logical_to_physical_map[lba][parity_drive_1.drive_id] = physical_sector_p
                logger.log(
                    f"RAID-60 Subarray {i}: P-Parity '{p_parity_char}' stored on drive {parity_drive_1.drive_id}"
                )
            except Exception as e:
                logger.log(
                    f"Error writing P-parity to Drive {parity_drive_1.drive_id} in subarray {i}: {e}",
                    "ERROR",
                )
                parity_drive_1.mark_failed()
                raise

            try:
                physical_sector_q = parity_drive_2.write_sector(q_parity_char, "PARITY-Q", lba)
                self.logical_to_physical_map[lba][parity_drive_2.drive_id] = physical_sector_q
                logger.log(
                    f"RAID-60 Subarray {i}: Q-Parity '{q_parity_char}' stored on drive {parity_drive_2.drive_id}"
                )
            except Exception as e:
                logger.log(
                    f"Error writing Q-parity to Drive {parity_drive_2.drive_id} in subarray {i}: {e}",
                    "ERROR",
                )
                parity_drive_2.mark_failed()
                raise
            time.sleep(0.2)

        logger.log(f"RAID-60: Data '{data}' written to striped RAID-6 subarrays for LBA {lba}")


    def _calculate_parity(self, data: str) -> str:
        """Simple XOR parity calculation for demonstration"""
        if not data:
            return "0000"

        parity_val = 0
        for char in data:
            try:
                parity_val ^= ord(char)
            except TypeError:
                logger.log(f"WARNING: Non-character data in parity calculation: '{char}'", "WARN")
                pass

        parity_str = f"P{parity_val:03d}"[:4]
        return parity_str

    def start_rebuild(self, failed_logical_drive_position: int, replacement_drive_id: int):
        """Start the rebuild process for a specific logical drive position to a new replacement drive."""
        if self.rebuild_active:
            logger.log("REBUILD: Another rebuild is already active.", "WARN")
            return

        # Validate drive IDs
        if not (0 <= failed_logical_drive_position < len(self.drives)):
            logger.log(f"Invalid failed logical drive position: {failed_logical_drive_position}", "ERROR")
            return
        if not (0 <= replacement_drive_id < len(self.drives)):
            logger.log(f"Invalid replacement drive ID: {replacement_drive_id}", "ERROR")
            return

        failed_drive_obj = self.drives[failed_logical_drive_position]
        replacement_drive_obj = self.drives[replacement_drive_id]

        if failed_drive_obj.is_active and failed_logical_drive_position != replacement_drive_id:
            logger.log(
                f"Cannot rebuild for drive at logical position {failed_logical_drive_position} as it is still active.",
                "WARN",
            )
            return

        if replacement_drive_obj.is_active and failed_logical_drive_position != replacement_drive_id:
            logger.log(f"Replacement drive {replacement_drive_id} is already active.", "WARN")
            return

        # Clear and prepare the replacement drive
        replacement_drive_obj.sectors = {}
        replacement_drive_obj.next_physical_sector = 0
        replacement_drive_obj.is_active = True
        replacement_drive_obj.metadata["status"] = "rebuilding"
        replacement_drive_obj._update_file()

        self.rebuild_active = True
        self.rebuild_thread = threading.Thread(
            target=self._rebuild_worker,
            args=(failed_logical_drive_position, replacement_drive_id),
            daemon=True,
        )
        self.rebuild_thread.start()
        logger.log(
            f"Started rebuild: Failed logical position {failed_logical_drive_position} -> Drive {replacement_drive_id}"
        )

    def _rebuild_worker(self, failed_logical_drive_position: int, replacement_drive_id: int):
        """Rebuild worker thread."""
        try:
            replacement_drive = self.drives[replacement_drive_id]

            logger.log(
                f"REBUILD: Starting rebuild for logical drive position {failed_logical_drive_position} to replacement drive {replacement_drive_id}"
            )

            total_logical_blocks = self.current_logical_block_index
            if total_logical_blocks == 0:
                logger.log("REBUILD: No data blocks to rebuild.", "INFO")
                replacement_drive.metadata["status"] = "active"
                replacement_drive._update_file()
                self._save_config()
                return

            for lba in range(total_logical_blocks):
                if not self.rebuild_active:
                    break

                time.sleep(0.5)

                physical_sector_map_for_lba = self.logical_to_physical_map.get(lba, {})

                target_physical_sector_on_replacement = physical_sector_map_for_lba.get(
                    failed_logical_drive_position
                )

                if target_physical_sector_on_replacement is None:
                    logger.log(
                        f"REBUILD WARN: Logical block {lba} did not have a corresponding physical sector on the failed logical position {failed_logical_drive_position}, skipping.",
                        "INFO",
                    )
                    continue

                rebuilt_data = None

                if self.raid_level == 0:
                    logger.log(
                        f"REBUILD ERROR: RAID-0 cannot be rebuilt, data permanently lost for LBA {lba} on failed drive {failed_logical_drive_position}.",
                        "ERROR",
                    )
                    replacement_drive.write_to_specific_sector(
                        target_physical_sector_on_replacement,
                        "LOST",
                        "PERM_LOST",
                        lba,
                    )
                    self.logical_to_physical_map[lba][replacement_drive_id] = -1
                    continue

                elif self.raid_level == 1:
                    for drive in self.drives:
                        if drive.is_active and drive.drive_id != failed_logical_drive_position:
                            mirror_physical_sector = physical_sector_map_for_lba.get(
                                drive.drive_id
                            )
                            if mirror_physical_sector is not None:
                                data = drive.read_sector(mirror_physical_sector)
                                if data is not None:
                                    rebuilt_data = data
                                    break
                                else:
                                    logger.log(
                                        f"REBUILD WARN: Could not read data from drive {drive.drive_id} for LBA {lba} at physical sector {mirror_physical_sector}",
                                        "WARN",
                                    )

                elif self.raid_level == 5:
                    original_total_drives = len(self.drives)
                    parity_drive_index_for_lba = lba % original_total_drives

                    data_blocks_for_reconstruct = []
                    parity_block_for_reconstruct = None

                    for d_idx, drive in enumerate(self.drives):
                        if drive.drive_id == replacement_drive_id or not drive.is_active:
                            continue

                        physical_sector_num = physical_sector_map_for_lba.get(drive.drive_id)
                        if physical_sector_num is None or physical_sector_num == -1:
                            continue

                        data_from_drive = drive.read_sector(physical_sector_num)
                        if data_from_drive is not None:
                            if d_idx == parity_drive_index_for_lba:
                                parity_block_for_reconstruct = data_from_drive
                            else:
                                data_blocks_for_reconstruct.append(data_from_drive)
                        else:
                            logger.log(
                                f"REBUILD WARN: Could not read data from drive {drive.drive_id} for LBA {lba} at physical sector {physical_sector_num}",
                                "WARN",
                            )

                    reconstructed_val = 0
                    for char_data in data_blocks_for_reconstruct:
                        try:
                            reconstructed_val ^= ord(char_data)
                        except TypeError:
                            logger.log(
                                f"REBUILD WARN: Non-character data '{char_data}' for XOR in LBA {lba}",
                                "WARN",
                            )
                            pass

                    if parity_block_for_reconstruct and parity_block_for_reconstruct.startswith('P'):
                        try:
                            parity_val_from_str = int(parity_block_for_reconstruct[1:])
                            reconstructed_val ^= parity_val_from_str
                        except ValueError:
                            logger.log(
                                f"REBUILD WARN: Invalid parity format '{parity_block_for_reconstruct}' in LBA {lba}",
                                "WARN",
                            )

                    if failed_logical_drive_position != parity_drive_index_for_lba:
                        try:
                            rebuilt_data = chr(reconstructed_val % 128)
                        except ValueError:
                            rebuilt_data = "???"
                    else:
                        rebuilt_data = self._calculate_parity("".join(data_blocks_for_reconstruct))

                    if not data_blocks_for_reconstruct and not parity_block_for_reconstruct:
                        logger.log(
                            f"REBUILD WARN: No sufficient data/parity to reconstruct LBA {lba} for RAID-5.",
                            "WARN",
                        )
                        rebuilt_data = "???"

                elif self.raid_level == 6:
                    original_total_drives = len(self.drives)
                    parity_drive_1_idx_for_lba = lba % original_total_drives
                    parity_drive_2_idx_for_lba = (lba + 1) % original_total_drives
                    if parity_drive_1_idx_for_lba == parity_drive_2_idx_for_lba:
                        parity_drive_2_idx_for_lba = (parity_drive_2_idx_for_lba + 1) % original_total_drives

                    data_blocks_for_reconstruct = []
                    p_parity_block = None
                    q_parity_block = None

                    for d_idx, drive in enumerate(self.drives):
                        if drive.drive_id == replacement_drive_id or not drive.is_active:
                            continue

                        physical_sector_num = physical_sector_map_for_lba.get(drive.drive_id)
                        if physical_sector_num is None or physical_sector_num == -1:
                            continue

                        data_from_drive = drive.read_sector(physical_sector_num)
                        if data_from_drive is not None:
                            if d_idx == parity_drive_1_idx_for_lba:
                                p_parity_block = data_from_drive
                            elif d_idx == parity_drive_2_idx_for_lba:
                                q_parity_block = data_from_drive
                            else:
                                data_blocks_for_reconstruct.append(data_from_drive)
                        else:
                            logger.log(
                                f"REBUILD WARN: Could not read data from drive {drive.drive_id} for LBA {lba} at physical sector {physical_sector_num}",
                                "WARN",
                            )

                    # Simplified RAID-6 reconstruction logic (conceptual)
                    if (
                        failed_logical_drive_position != parity_drive_1_idx_for_lba
                        and failed_logical_drive_position != parity_drive_2_idx_for_lba
                    ):
                        # A data drive failed, use remaining data and parity to reconstruct
                        # This would involve solving for the missing data block using P and Q
                        # For simplicity, if P parity and enough data is present, use P.
                        # If two drives failed, it becomes more complex, requiring Q.
                        reconstructed_val = 0
                        for char_data in data_blocks_for_reconstruct:
                            try:
                                reconstructed_val ^= ord(char_data)
                            except TypeError:
                                pass
                        if p_parity_block and p_parity_block.startswith('P'):
                            try:
                                parity_val_from_str = int(p_parity_block[1:])
                                reconstructed_val ^= parity_val_from_str
                            except ValueError:
                                pass
                        try:
                            rebuilt_data = chr(reconstructed_val % 128)
                        except ValueError:
                            rebuilt_data = "???"
                    elif failed_logical_drive_position == parity_drive_1_idx_for_lba:
                        # P parity drive failed, recalculate P
                        rebuilt_data = self._calculate_parity("".join(data_blocks_for_reconstruct))
                    elif failed_logical_drive_position == parity_drive_2_idx_for_lba:
                        # Q parity drive failed, recalculate Q
                        q_recalc_val = 0
                        for k, val in enumerate([ord(c) for c in data_blocks_for_reconstruct]):
                            q_recalc_val ^= (val << k)
                        rebuilt_data = f"Q{q_recalc_val % 1000:03d}"[:4]

                    if not data_blocks_for_reconstruct and not p_parity_block and not q_parity_block:
                        logger.log(
                            f"REBUILD WARN: No sufficient data/parity to reconstruct LBA {lba} for RAID-6.",
                            "WARN",
                        )
                        rebuilt_data = "???"

                elif self.raid_level == 10:
                    # For RAID 10, a drive fails within a mirrored pair.
                    # We look at the other active drive in the same pair for the data.
                    # First, identify the pair the failed drive belongs to.
                    mirrored_pairs: List[List[Drive]] = []
                    for i in range(0, len(self.drives), 2):
                        mirrored_pairs.append([self.drives[i], self.drives[i + 1]])

                    failed_pair_drives: Optional[List[Drive]] = None
                    for pair in mirrored_pairs:
                        if failed_logical_drive_position in [d.drive_id for d in pair]:
                            failed_pair_drives = pair
                            break

                    if failed_pair_drives:
                        for drive in failed_pair_drives:
                            if (
                                drive.is_active
                                and drive.drive_id != failed_logical_drive_position
                            ):  # The active mirror
                                mirror_physical_sector = physical_sector_map_for_lba.get(
                                    drive.drive_id
                                )
                                if mirror_physical_sector is not None:
                                    data = drive.read_sector(mirror_physical_sector)
                                    if data is not None:
                                        rebuilt_data = data
                                        break
                                else:
                                    logger.log(
                                        f"REBUILD WARN: Could not read data from mirror drive {drive.drive_id} for LBA {lba}",
                                        "WARN",
                                    )
                    if rebuilt_data is None:
                        logger.log(
                            f"REBUILD ERROR: Failed to reconstruct data for LBA {lba} in RAID-10. Both mirrors in pair may have failed.",
                            "ERROR",
                        )
                        rebuilt_data = "???"

                elif self.raid_level == 50:
                    # For RAID-50, rebuild is essentially a RAID-5 rebuild within a sub-array.
                    # Identify which sub-array the failed drive belongs to.
                    min_drives_for_subarray = self.raid_configs[5]["min_drives"]
                    subarray_index = failed_logical_drive_position // min_drives_for_subarray
                    
                    # Create a conceptual RAID-5 like environment for the subarray
                    subarray_start_id = subarray_index * min_drives_for_subarray
                    subarray_drives_global_ids = list(range(subarray_start_id, subarray_start_id + min_drives_for_subarray))

                    # Identify parity drive position within this subarray's active drives
                    active_subarray_drives = [d for d in self.drives if d.drive_id in subarray_drives_global_ids and d.is_active]
                    
                    if not active_subarray_drives:
                        logger.log(f"REBUILD ERROR: RAID-50 Subarray {subarray_index} completely failed, cannot reconstruct LBA {lba}.", "ERROR")
                        rebuilt_data = "???"
                    else:
                        # Find the relative index of the failed drive within the *original* subarray structure
                        relative_failed_idx = failed_logical_drive_position - subarray_start_id
                        
                        # Re-implement RAID-5 rebuild logic focusing on this subarray
                        parity_drive_relative_idx_for_lba = lba % len(active_subarray_drives) # Relative index within active subarray drives
                        # Map this relative index back to a global drive ID to get the actual parity drive
                        
                        data_blocks_for_reconstruct_subarray = []
                        parity_block_for_reconstruct_subarray = None
                        
                        for drive_id in subarray_drives_global_ids:
                            drive = next((d for d in self.drives if d.drive_id == drive_id), None)
                            if drive and drive.is_active and drive.drive_id != replacement_drive_id:
                                physical_sector_num = physical_sector_map_for_lba.get(drive.drive_id)
                                if physical_sector_num is not None and physical_sector_num != -1:
                                    data_from_drive = drive.read_sector(physical_sector_num)
                                    if data_from_drive:
                                        if drive.drive_id == active_subarray_drives[parity_drive_relative_idx_for_lba].drive_id:
                                            parity_block_for_reconstruct_subarray = data_from_drive
                                        else:
                                            data_blocks_for_reconstruct_subarray.append(data_from_drive)
                        
                        reconstructed_val = 0
                        for char_data in data_blocks_for_reconstruct_subarray:
                            try:
                                reconstructed_val ^= ord(char_data)
                            except TypeError:
                                pass
                        
                        if parity_block_for_reconstruct_subarray and parity_block_for_reconstruct_subarray.startswith('P'):
                            try:
                                parity_val_from_str = int(parity_block_for_reconstruct_subarray[1:])
                                reconstructed_val ^= parity_val_from_str
                            except ValueError:
                                pass
                        
                        if relative_failed_idx != (active_subarray_drives[parity_drive_relative_idx_for_lba].drive_id - subarray_start_id):
                            try:
                                rebuilt_data = chr(reconstructed_val % 128)
                            except ValueError:
                                rebuilt_data = "???"
                        else:
                            rebuilt_data = self._calculate_parity("".join(data_blocks_for_reconstruct_subarray))

                        if not data_blocks_for_reconstruct_subarray and not parity_block_for_reconstruct_subarray:
                            logger.log(f"REBUILD WARN: RAID-50 Subarray {subarray_index}: No sufficient data/parity to reconstruct LBA {lba}.", "WARN")
                            rebuilt_data = "???"

                elif self.raid_level == 60:
                    # For RAID-60, rebuild is essentially a RAID-6 rebuild within a sub-array.
                    min_drives_for_subarray = self.raid_configs[6]["min_drives"]
                    subarray_index = failed_logical_drive_position // min_drives_for_subarray

                    subarray_start_id = subarray_index * min_drives_for_subarray
                    subarray_drives_global_ids = list(range(subarray_start_id, subarray_start_id + min_drives_for_subarray))

                    active_subarray_drives = [d for d in self.drives if d.drive_id in subarray_drives_global_ids and d.is_active]

                    if not active_subarray_drives:
                        logger.log(f"REBUILD ERROR: RAID-60 Subarray {subarray_index} completely failed, cannot reconstruct LBA {lba}.", "ERROR")
                        rebuilt_data = "???"
                    else:
                        relative_failed_idx = failed_logical_drive_position - subarray_start_id
                        
                        parity_drive_1_relative_idx_for_lba = lba % len(active_subarray_drives)
                        parity_drive_2_relative_idx_for_lba = (lba + 1) % len(active_subarray_drives)
                        if parity_drive_1_relative_idx_for_lba == parity_drive_2_relative_idx_for_lba:
                            parity_drive_2_relative_idx_for_lba = (parity_drive_2_relative_idx_for_lba + 1) % len(active_subarray_drives)
                        
                        data_blocks_for_reconstruct_subarray = []
                        p_parity_block_subarray = None
                        q_parity_block_subarray = None

                        for drive_id in subarray_drives_global_ids:
                            drive = next((d for d in self.drives if d.drive_id == drive_id), None)
                            if drive and drive.is_active and drive.drive_id != replacement_drive_id:
                                physical_sector_num = physical_sector_map_for_lba.get(drive.drive_id)
                                if physical_sector_num is not None and physical_sector_num != -1:
                                    data_from_drive = drive.read_sector(physical_sector_num)
                                    if data_from_drive:
                                        if drive.drive_id == active_subarray_drives[parity_drive_1_relative_idx_for_lba].drive_id:
                                            p_parity_block_subarray = data_from_drive
                                        elif drive.drive_id == active_subarray_drives[parity_drive_2_relative_idx_for_lba].drive_id:
                                            q_parity_block_subarray = data_from_drive
                                        else:
                                            data_blocks_for_reconstruct_subarray.append(data_from_drive)
                        
                        # Simplified RAID-6 reconstruction logic for subarray
                        if (
                            relative_failed_idx != (active_subarray_drives[parity_drive_1_relative_idx_for_lba].drive_id - subarray_start_id)
                            and relative_failed_idx != (active_subarray_drives[parity_drive_2_relative_idx_for_lba].drive_id - subarray_start_id)
                        ):
                            reconstructed_val = 0
                            for char_data in data_blocks_for_reconstruct_subarray:
                                try:
                                    reconstructed_val ^= ord(char_data)
                                except TypeError:
                                    pass
                            if p_parity_block_subarray and p_parity_block_subarray.startswith('P'):
                                try:
                                    parity_val_from_str = int(p_parity_block_subarray[1:])
                                    reconstructed_val ^= parity_val_from_str
                                except ValueError:
                                    pass
                            try:
                                rebuilt_data = chr(reconstructed_val % 128)
                            except ValueError:
                                rebuilt_data = "???"
                        elif relative_failed_idx == (active_subarray_drives[parity_drive_1_relative_idx_for_lba].drive_id - subarray_start_id):
                            rebuilt_data = self._calculate_parity("".join(data_blocks_for_reconstruct_subarray))
                        elif relative_failed_idx == (active_subarray_drives[parity_drive_2_relative_idx_for_lba].drive_id - subarray_start_id):
                            q_recalc_val = 0
                            for k, val in enumerate([ord(c) for c in data_blocks_for_reconstruct_subarray]):
                                q_recalc_val ^= (val << k)
                            rebuilt_data = f"Q{q_recalc_val % 1000:03d}"[:4]

                        if not data_blocks_for_reconstruct_subarray and not p_parity_block_subarray and not q_parity_block_subarray:
                            logger.log(f"REBUILD WARN: RAID-60 Subarray {subarray_index}: No sufficient data/parity to reconstruct LBA {lba}.", "WARN")
                            rebuilt_data = "???"


                if rebuilt_data is not None:
                    replacement_drive.write_to_specific_sector(
                        target_physical_sector_on_replacement, rebuilt_data, "REBUILT", lba
                    )
                    self.logical_to_physical_map[lba][replacement_drive_id] = target_physical_sector_on_replacement
                else:
                    logger.log(
                        f"REBUILD ERROR: Failed to reconstruct data for LBA {lba} on failed logical position {failed_logical_drive_position}",
                        "ERROR",
                    )
                    replacement_drive.write_to_specific_sector(
                        target_physical_sector_on_replacement, "ERROR", "REBUILD-FAIL", lba
                    )

                progress = ((lba + 1) / total_logical_blocks) * 100
                logger.log(f"REBUILD: Progress {progress:.1f}% - Logical Block {lba}")

            replacement_drive.metadata["status"] = "active"
            replacement_drive._update_file()
            self._save_config()
            logger.log("REBUILD: Drive rebuild completed successfully")

        except Exception as e:
            logger.log(f"REBUILD: Error during rebuild - {e}", "ERROR")

        finally:
            self.rebuild_active = False
            self.health_check()  # Run a health check after rebuild

    def health_check(self):
        """
        Performs a health check on the RAID array:
        - Checks if all active drives have the expected number of mapped physical sectors.
        - Checks for data consistency (conceptual, for demo, verifies presence not content).
        - Identifies if a rebuild is in progress or recommended.
        """
        print(f"\n{'='*60}")
        print("RAID HEALTH CHECK")
        print(f"{'='*60}")
        health_status = "OK"

        # 1. Check active drive count vs fault tolerance
        active_drives_count = sum(1 for d in self.drives if d.is_active)
        fault_tolerance = self.raid_configs[self.raid_level]["fault_tolerance"]

        if active_drives_count < (len(self.drives) - fault_tolerance):
            print("STATUS: CRITICAL - RAID has failed beyond fault tolerance.")
            health_status = "CRITICAL"
        elif active_drives_count < len(self.drives):
            print("STATUS: DEGRADED - One or more drives have failed. Rebuild recommended.")
            health_status = "DEGRADED (Rebuild Recommended)"
        else:
            print("STATUS: HEALTHY - All configured drives are active.")

        # 2. Check rebuild status
        if self.rebuild_active:
            print("REBUILD: In progress.")
            health_status = "DEGRADED (Rebuild In Progress)"
        elif "DEGRADED" in health_status and not self.rebuild_active:
            print("REBUILD: Recommended - No rebuild is currently active despite degraded state.")

        # 3. Drive-specific checks (missing files, signature mismatches from _load_config)
        for drive in self.drives:
            if drive.metadata.get("status") == "failed_signature_mismatch":
                print(f"DRIVE {drive.drive_id}: FAILED (Signature Mismatch) - Replacement/Rebuild required.")
                health_status = "DEGRADED (Signature Mismatch)"
            elif not drive.is_active and drive.metadata.get("status") != "failed":
                # This would catch drives that are logically inactive but not marked 'failed'
                print(f"DRIVE {drive.drive_id}: INACTIVE - Status: {drive.metadata['status']}")
                health_status = "DEGRADED (Inactive Drive)"
            elif not os.path.exists(drive.file_path) and drive.is_active:
                print(f"DRIVE {drive.drive_id}: CRITICAL - Drive file missing but marked active! Marking as failed.")
                drive.mark_failed()  # Mark it failed immediately
                health_status = "CRITICAL"

        # 4. Data Consistency/Block Mapping Check
        print("\nDATA CONSISTENCY CHECK:")
        inconsistent_blocks = []
        for lba in range(self.current_logical_block_index):
            mapped_drives_for_lba = self.logical_to_physical_map.get(lba, {})

            # Count how many active drives *should* have data for this LBA
            expected_data_sources = 0
            actual_data_present = 0

            # General check: Iterate through all drives that *should* have a block for this LBA
            # based on self.logical_to_physical_map
            for drive_id, p_sector in mapped_drives_for_lba.items():
                if drive_id < len(self.drives):
                    drive_obj = self.drives[drive_id]
                    if drive_obj.is_active and p_sector != -1:  # Check if actively mapped and not permanently lost
                        if p_sector in drive_obj.sectors:
                            actual_data_present += 1
                        else:
                            # Mapped, active, but sector not found in drive's sectors
                            logger.log(
                                f"LBA {lba}, Drive {drive_id}: Mapped physical sector {p_sector} not found on active drive's internal sectors.",
                                "WARN",
                            )
                    elif p_sector == -1:
                        logger.log(
                            f"LBA {lba}, Drive {drive_id}: Marked as permanently lost block.", "INFO"
                        )

            if self.raid_level == 0:
                # RAID-0: Each character goes to one drive. If any one is missing (marked -1 or truly missing), it's lost.
                # If a drive in a RAID-0 array fails, the entire logical drive becomes unusable.
                # Here, we assume a "permanently_failed" status for RAID-0 means its contribution to LBAs is lost.
                if any(p_sector == -1 for p_sector in mapped_drives_for_lba.values()):
                    inconsistent_blocks.append(
                        f"LBA {lba} (RAID-0: Permanently lost due to drive failure)"
                    )
                    if health_status == "OK" or health_status == "INCONSISTENT":
                        health_status = "CRITICAL"
                elif actual_data_present < len(mapped_drives_for_lba):
                    # Some parts are missing but not yet marked -1, degraded.
                    inconsistent_blocks.append(
                        f"LBA {lba} (RAID-0: Missing data on {len(mapped_drives_for_lba) - actual_data_present} drives)"
                    )
                    if health_status == "OK":
                        health_status = "INCONSISTENT"

            elif self.raid_level == 1:
                # RAID-1: All drives should have copies.
                total_mirrors = len(self.drives)
                if actual_data_present < (total_mirrors - fault_tolerance):
                    # If we have less than (total - FT) copies, it's critical.
                    # e.g., for FT=1, if 0 copies left, it's critical. If 1 copy left, it's degraded.
                    inconsistent_blocks.append(
                        f"LBA {lba} (RAID-1: CRITICAL data loss, {total_mirrors - actual_data_present} copies missing)"
                    )
                    if health_status == "OK" or health_status == "DEGRADED (Rebuild Recommended)":
                        health_status = "CRITICAL"
                elif actual_data_present < total_mirrors:
                    # Some mirrors are missing but still recoverable (within fault tolerance)
                    inconsistent_blocks.append(
                        f"LBA {lba} (RAID-1: DEGRADED, {total_mirrors - actual_data_present} mirror copies missing)"
                    )
                    if health_status == "OK":
                        health_status = "INCONSISTENT"  # Needs rebuild to become consistent

            elif self.raid_level == 5:
                # RAID-5: Needs (N-1) drives to reconstruct. (N-1) is total drives minus parity.
                # If only one drive fails, it's still reconstructable. If two fail, it's critical.
                # Number of drives that *originally stored* data for this LBA, excluding parity.
                # Then sum up available data+parity for reconstruction.
                num_total_stripe_members = len(self.drives) # All drives participate in the stripe, data or parity
                
                if actual_data_present < (num_total_stripe_members - fault_tolerance):
                     inconsistent_blocks.append(f"LBA {lba} (RAID-5: CRITICAL data loss, {num_total_stripe_members - actual_data_present} sources missing. Beyond fault tolerance.)")
                     if health_status == "OK" or health_status == "DEGRADED (Rebuild Recommended)": health_status = "CRITICAL"
                elif actual_data_present < num_total_stripe_members: # Some missing, but reconstructable within FT
                     inconsistent_blocks.append(f"LBA {lba} (RAID-5: Degraded, {num_total_stripe_members - actual_data_present} sources missing. Rebuildable.)")
                     if health_status == "OK": health_status = "INCONSISTENT"

            elif self.raid_level == 6:
                # RAID-6: Needs (N-2) drives to reconstruct (supports 2 drive failures).
                num_total_stripe_members = len(self.drives)
                if actual_data_present < (num_total_stripe_members - fault_tolerance):
                    inconsistent_blocks.append(f"LBA {lba} (RAID-6: CRITICAL data loss, {num_total_stripe_members - actual_data_present} sources missing. Beyond fault tolerance.)")
                    if health_status == "OK" or health_status == "DEGRADED (Rebuild Recommended)": health_status = "CRITICAL"
                elif actual_data_present < num_total_stripe_members:
                    inconsistent_blocks.append(f"LBA {lba} (RAID-6: Degraded, {num_total_stripe_members - actual_data_present} sources missing. Rebuildable.)")
                    if health_status == "OK": health_status = "INCONSISTENT"

            elif self.raid_level == 10:
                # RAID-10: Each mirrored pair must have at least one active drive for its part of the stripe.
                # Overall fault tolerance is higher than 2, but depends on which drives fail.
                min_drives_per_pair = 1 # At least one drive active in a mirrored pair
                num_mirrored_pairs = len(self.drives) // 2
                
                pairs_with_data_loss = 0
                for i in range(num_mirrored_pairs):
                    pair_drives_global_ids = [self.drives[2*i].drive_id, self.drives[2*i+1].drive_id]
                    active_in_pair = 0
                    for d_id in pair_drives_global_ids:
                        if d_id in mapped_drives_for_lba and self.drives[d_id].is_active and mapped_drives_for_lba[d_id] != -1:
                            if mapped_drives_for_lba[d_id] in self.drives[d_id].sectors:
                                active_in_pair += 1
                    
                    if active_in_pair < min_drives_per_pair:
                        # If a pair has less than min_drives_per_pair active/readable, it has lost data.
                        pairs_with_data_loss += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-10: Mirrored pair {pair_drives_global_ids} has data loss for this block.)")
                        if health_status == "OK" or health_status == "INCONSISTENT":
                            health_status = "CRITICAL"
                    elif active_in_pair < 2: # One drive in pair failed, still reconstructable within pair
                        inconsistent_blocks.append(f"LBA {lba} (RAID-10: Mirrored pair {pair_drives_global_ids} degraded for this block. Rebuildable.)")
                        if health_status == "OK": health_status = "INCONSISTENT"

            elif self.raid_level == 50:
                # RAID-50: Striped RAID-5. Each sub-array is RAID-5.
                min_drives_for_subarray = self.raid_configs[5]["min_drives"]
                subarray_fault_tolerance = self.raid_configs[5]["fault_tolerance"]

                num_subarrays = len(self.drives) // min_drives_for_subarray
                
                critical_subarrays = 0
                degraded_subarrays = 0

                for i in range(num_subarrays):
                    subarray_start_id = i * min_drives_for_subarray
                    subarray_end_id = subarray_start_id + min_drives_for_subarray
                    subarray_drives_global_ids = list(range(subarray_start_id, subarray_end_id))

                    active_data_sources_in_subarray = 0
                    for d_id in subarray_drives_global_ids:
                        if d_id in mapped_drives_for_lba and self.drives[d_id].is_active and mapped_drives_for_lba[d_id] != -1:
                            if mapped_drives_for_lba[d_id] in self.drives[d_id].sectors:
                                active_data_sources_in_subarray += 1
                    
                    if active_data_sources_in_subarray < (min_drives_for_subarray - subarray_fault_tolerance):
                        critical_subarrays += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-50 Subarray {i}: CRITICAL data loss. Beyond fault tolerance.)")
                    elif active_data_sources_in_subarray < min_drives_for_subarray:
                        degraded_subarrays += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-50 Subarray {i}: Degraded. Rebuildable.)")
                
                if critical_subarrays > 0:
                    health_status = "CRITICAL"
                elif degraded_subarrays > 0 and health_status == "OK":
                    health_status = "INCONSISTENT"

            elif self.raid_level == 60:
                # RAID-60: Striped RAID-6. Each sub-array is RAID-6.
                min_drives_for_subarray = self.raid_configs[6]["min_drives"]
                subarray_fault_tolerance = self.raid_configs[6]["fault_tolerance"]

                num_subarrays = len(self.drives) // min_drives_for_subarray
                
                critical_subarrays = 0
                degraded_subarrays = 0

                for i in range(num_subarrays):
                    subarray_start_id = i * min_drives_for_subarray
                    subarray_end_id = subarray_start_id + min_drives_for_subarray
                    subarray_drives_global_ids = list(range(subarray_start_id, subarray_end_id))

                    active_data_sources_in_subarray = 0
                    for d_id in subarray_drives_global_ids:
                        if d_id in mapped_drives_for_lba and self.drives[d_id].is_active and mapped_drives_for_lba[d_id] != -1:
                            if mapped_drives_for_lba[d_id] in self.drives[d_id].sectors:
                                active_data_sources_in_subarray += 1
                    
                    if active_data_sources_in_subarray < (min_drives_for_subarray - subarray_fault_tolerance):
                        critical_subarrays += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-60 Subarray {i}: CRITICAL data loss. Beyond fault tolerance.)")
                    elif active_data_sources_in_subarray < min_drives_for_subarray:
                        degraded_subarrays += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-60 Subarray {i}: Degraded. Rebuildable.)")
                
                if critical_subarrays > 0:
                    health_status = "CRITICAL"
                elif degraded_subarrays > 0 and health_status == "OK":
                    health_status = "INCONSISTENT"


        if inconsistent_blocks:
            print("\nINCONSISTENT BLOCKS DETECTED:")
            for block_info in inconsistent_blocks:
                print(f"- {block_info}")
            print("REBUILD RECOMMENDED to restore consistency if degraded, or CRITICAL data loss suspected.")
        else:
            print("All mapped logical blocks appear to be present on active drives.")

        print(f"\nOVERALL HEALTH: {health_status}")
        print(f"{'='*60}")
        return health_status

    def display_status(self):
        """Display current RAID status and then run a health check."""

        print(f"\n{'='*60}")
        print(f"RAID-{self.raid_level} STATUS")
        print(f"{'='*60}")
        print(f"Configuration: {self.raid_configs[self.raid_level]['name']}")
        print(f"RAID Signature: {self.raid_signature}")
        print(f"Total Configured Drives: {len(self.drives)}")  # Refers to all drives in config, active or not
        print(f"Active Drives: {sum(1 for d in self.drives if d.is_active)}")
        print(f"Failed Drives: {sum(1 for d in self.drives if not d.is_active)}")
        print(f"Current Logical Block Index: {self.current_logical_block_index}")
        print(f"Rebuild Active: {'Yes' if self.rebuild_active else 'No'}")
        print()

        for drive in self.drives:
            status = drive.metadata["status"]  # Use the status from metadata for more detail
            print(
                f"Drive {drive.drive_id}: {status} - {drive.next_physical_sector} physical sectors written (Signature: {drive.signature})"
            )

        print(f"\nLogical Block to Physical Sector Mapping:")
        if not self.logical_to_physical_map:
            print("  No logical blocks written yet.")
        else:
            for lba in sorted(self.logical_to_physical_map.keys()):
                drive_sector_map = self.logical_to_physical_map[lba]
                sorted_drive_sector_map = {
                    d_id: drive_sector_map[d_id] for d_id in sorted(drive_sector_map.keys())
                }
                print(f"  LBA {lba}: {sorted_drive_sector_map}")

        # Run health check here
        self.health_check()

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
        print("2. Simulate drive failure (and re-add/rebuild options)")
        print("3. Add new drive to RAID (without triggering rebuild)")
        print("4. View RAID status and perform health check")
        print("5. Clear current RAID configuration and re-initialize")
        print("6. Exit RAID demo")
        print()

        choice = input("Enter your choice (1-6): ").strip()

        if choice == "1":
            data = input("Enter data to write: ")
            if data:
                raid.write_data(data)
                print("Press Enter to continue...")
                input()

        elif choice == "2":
            raid.display_status()
            try:
                drive_id_to_fail = int(input("Enter drive ID to simulate failure for: "))
                raid.remove_drive(drive_id_to_fail)
            except ValueError:
                print("Invalid drive ID (must be a number).")
            print("Press Enter to continue...")
            input()

        elif choice == "3":
            drive_id = raid.add_drive()
            print(
                f"Added drive {drive_id}. (This does not trigger a rebuild automatically. Use option 2 to start a rebuild for a failed drive.)"
            )
            print("Press Enter to continue...")
            input()

        elif choice == "4":
            raid.display_status()  # This now includes the health check
            print("\nDrive files and raid_config.json created in folder:", raid.folder_path)
            print("You can view these files to see detailed block layouts and RAID state.")
            print("Press Enter to continue...")
            input()

        elif choice == "5":
            print("\nWARNING: This will DELETE the current RAID configuration and all simulated drive data!")
            confirm = input("Are you sure you want to clear the configuration? (y/n): ").strip().lower()
            if confirm == "y":
                raid.cleanup()  # Clean up current RAID threads
                # The logic for clearing the folder and re-initializing is handled in main()
                logger.log(f"Initiating clear of RAID configuration {raid.folder_path}.", "INFO")
                return "clear_config"  # Special return to trigger re-initialization in main()
            else:
                print("Clear configuration cancelled.")
            print("Press Enter to continue...")
            input()

        elif choice == "6":
            break

        else:
            print("Invalid choice. Please try again.")
    return None

# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------

def main():
    """Main program entry point"""
    global logging_active

    global logger
    logger = Logger()

    print("RAID Visualization")
    print("=" * 40)
    logger.log("RAIDVIZ started")

    try:
        while True:
            print("\nAvailable RAID Levels:")
            print("0 - RAID-0 (Striping)")
            print("1 - RAID-1 (Mirroring)")
            print("5 - RAID-5 (Striping with Parity)")
            print("6 - RAID-6 (Striping with Dual Parity)")
            print("10 - RAID-10 (Mirrored Stripes)")
            print("50 - RAID-50 (Striped RAID-5)")
            print("60 - RAID-60 (Striped RAID-6)")
            print("q - Quit")

            choice = input("\nSelect RAID level for demonstration (or q to quit): ").strip().lower()

            if choice == "q":
                break

            try:
                raid_level = int(choice)
                if raid_level in [0, 1, 5, 6, 10, 50, 60]:
                    raid = RAIDArray(raid_level)  # Create instance here, but don't auto-init

                    raid_folder = f"raid_{raid_level}"
                    config_exists = os.path.exists(raid.config_file)

                    clear_on_next_init = True
                    if config_exists:
                        print(f"\nExisting RAID-{raid_level} configuration found in '{raid_folder}'.")
                        prompt = input(
                            "Do you want to clear it and start fresh? (y/n, default 'n' to load): "
                        ).strip().lower()
                        if prompt == "y":
                            clear_on_next_init = True
                            # Force delete folder now if user wants to clear
                            if os.path.exists(raid_folder):
                                shutil.rmtree(raid_folder)
                                logger.log(f"User chose to clear: Removed {raid_folder}", "INFO")
                        else:
                            clear_on_next_init = False  # Attempt to load existing config

                    raid.initialize_raid_structure(clear_existing=clear_on_next_init)

                    result = interactive_mode(raid)
                    if result == "clear_config":
                        raid.cleanup()  # Ensure cleanup before restarting
                        continue  # Go back to RAID level selection

                    raid.cleanup()
                else:
                    print("Invalid RAID level. Please choose 0, 1, 5, 6, 10, 50, or 60.")
            except ValueError:
                print("Invalid input. Please enter a number or 'q' to quit.")

    except KeyboardInterrupt:
        print("\n\nShutting down...")

    finally:
        logging_active = False
        logger.log("shutting down")
        time.sleep(0.5)


if __name__ == "__main__":
    main()