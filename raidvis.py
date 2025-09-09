import os
import shutil
import threading
import time
import queue
import datetime
from typing import List, Dict, Optional, Any
import json
import random

# Global queue for logging messages to ensure thread-safe logging
log_queue = queue.Queue()
# Flag to control the logging thread's lifecycle
logging_active = True


class Logger:
    """
    Handles system logging to both the console and a file.
    This class ensures all messages are timestamped and categorized
    (INFO, WARN, ERROR) for better traceability.
    """

    def __init__(self, log_file="system.log"):
        self.log_file = log_file
        # Start a daemon thread for logging so it doesn't prevent program exit
        self.log_thread = threading.Thread(target=self._logging_worker, daemon=True)
        self.log_thread.start()

    def log(self, message: str, level: str = "INFO"):
        """
        Adds a message to the logging queue with a timestamp and level.
        Messages are then processed by the logging worker thread.
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{timestamp}] {level}: {message}"
        log_queue.put(formatted_msg)

    def _logging_worker(self):
        """
        The main worker function for the logging thread.
        It continuously pulls messages from the queue and writes them
        to the console and the log file.
        """
        with open(self.log_file, "a") as f:
            while logging_active:
                try:
                    message = log_queue.get(timeout=0.1)  # Wait briefly for messages
                    if "WARN" in message:
                        print(f"\033[93m{message}\033[0m")  # Yellow for warnings
                    elif "ERROR" in message:
                        print(f"\033[91m{message}\033[0m")  # Red for errors
                    else:
                        print(message)  # Default color for info
                    f.write(message + "\n")  # Write to file
                    f.flush()  # Ensure it's written to disk immediately
                except queue.Empty:
                    continue  # No messages, just check again


# ------------------------------------------
# Drive Class and definition
# ---------------------------------------------------------------------
class Drive:
    """
    Represents a single physical drive within the RAID array.
    Each drive manages its own sectors and metadata.
    """

    def __init__(
        self, drive_id: int, folder_path: str, signature: Optional[str] = None
    ):
        self.drive_id = drive_id
        self.folder_path = folder_path
        self.file_path = os.path.join(folder_path, f"disk_{drive_id}")
        self.is_active = True  # Indicates if the drive is operational
        # Dictionary to store sector data: physical_sector_number -> {data, type, lba}
        self.sectors: Dict[int, Dict[str, Any]] = {}
        self.next_physical_sector = 0  # Counter for the next available physical sector

        # A unique signature helps identify a drive, especially during rebuilds
        self.signature = (
            signature
            if signature
            else f"DRV-{drive_id}-{random.randint(10000,99999)}-{datetime.datetime.now().timestamp()}"
        )

        self.metadata = {
            "drive_id": drive_id,
            "creation_time": datetime.datetime.now().isoformat(),
            "status": "active",  # Current operational status
            "total_sectors": 0,  # Max sectors (not implemented for dynamic demo)
            "used_sectors": 0,
            "signature": self.signature,
        }
        self.create_drive_file()  # Create the physical file representation

    def create_drive_file(self):
        """
        Initializes the drive's representation file on disk with a header
        and metadata. This makes the simulated drive's content visible.
        """
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
                f.write(f"Part of RAID: [Will be updated]\n")  # Placeholder
                f.write(f"Position in RAID: [Will be updated]\n")  # Placeholder
                f.write(f"Rebuild Rate: N/A\n\n")  # Placeholder
                f.write("BLOCK DIAGRAM:\n")
                f.write("+--------+--------+--------+--------+\n")
                f.write("| Sector | LBlock | Type   | Data   |\n")
                f.write("+--------+--------+--------+--------+\n")
        except Exception as e:
            logger.log(f"Error creating drive file {self.file_path}: {e}", "ERROR")

    def write_sector(
        self, data: str, block_type: str = "DATA", lba: Optional[int] = None
    ):
        """
        Writes data to the next available physical sector on this drive.
        Associates the data with a Logical Block Address (LBA) if provided.
        """
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

        self._update_file()  # Update the on-disk file representation
        logger.log(
            f"Drive {self.drive_id}: Written '{data}' to physical sector {current_physical_sector} (LBA: {lba if lba is not None else 'N/A'}) as {block_type}"
        )
        return current_physical_sector

    def write_to_specific_sector(
        self,
        sector_num: int,
        data: str,
        block_type: str = "DATA",
        lba: Optional[int] = None,
    ):
        """
        Writes data to a specific physical sector. This is primarily used
        during rebuild operations to precisely place reconstructed data.
        """
        if not self.is_active:
            raise Exception(f"Drive {self.drive_id} is not active")

        self.sectors[sector_num] = {"data": data, "type": block_type, "lba": lba}
        self.metadata["used_sectors"] = len(self.sectors)
        # Make sure next_physical_sector is at least this sector + 1
        if sector_num >= self.next_physical_sector:
            self.next_physical_sector = sector_num + 1
        self._update_file()
        logger.log(
            f"Drive {self.drive_id}: (Rebuild) Written '{data}' to physical sector {sector_num} (LBA: {lba if lba is not None else 'N/A'}) as {block_type}"
        )

    def read_sector(self, sector: int) -> Optional[str]:
        """
        Reads and returns data from a specific physical sector.
        Handles cases where the drive is failed or the file is missing.
        """
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
        """Marks the drive as failed, making it inactive for I/O operations."""
        self.is_active = False
        self.metadata["status"] = "failed"
        logger.log(f"Drive {self.drive_id}: DRIVE FAILURE DETECTED", "ERROR")
        self._update_file()

    def _update_file(self):
        """
        Rewrites the drive's file on disk to reflect its current state,
        including metadata and the block diagram.
        """
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
                    # Show a preview of data, truncating if too long
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
            # If the file disappears while active, mark the drive as failed
            if self.is_active:
                logger.log(
                    f"Drive {self.drive_id} file not found during update. Marking as failed.",
                    "ERROR",
                )
                self.mark_failed()
        except Exception as e:
            logger.log(f"Error updating drive file {self.file_path}: {e}", "ERROR")


# ---------------------------------------------------------------------
# RAID Array Class and implementation
# ---------------------------------------------------------------------


class RAIDArray:
    """
    Manages a collection of drives as a single logical unit according to
    a specified RAID level. Handles data distribution, rebuilds, and status.
    """

    def __init__(self, raid_level: int):
        self.raid_level = raid_level
        self.drives: List[Drive] = []
        self.folder_path = f"raid_{raid_level}"
        self.rebuild_active = False
        self.rebuild_thread = None
        self.current_logical_block_index = 0
        # Maps LBA to a dictionary of {drive_id: physical_sector_number}
        self.logical_to_physical_map: Dict[int, Dict[int, int]] = {}

        self.raid_signature = (
            f"RAID-{raid_level}-{datetime.datetime.now().timestamp()}"
        )
        self.config_file = os.path.join(self.folder_path, "raid_config.json")

        # Define configurations for different RAID levels
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

    def initialize_raid_structure(self, clear_existing: bool = True):
        """
        Initializes the RAID array by setting up its folder, creating/loading
        drives, and managing the configuration file.
        If `clear_existing` is True, any old configuration is removed.
        """
        logger.log(
            f"Initializing {self.raid_configs[self.raid_level]['name']} structure (clear_existing={clear_existing})"
        )

        if os.path.exists(self.folder_path):
            if clear_existing:
                logger.log(f"Clearing existing RAID folder: {self.folder_path}")
                shutil.rmtree(self.folder_path)
            else:
                # If we're not clearing, we'll try to load, so no need to create folder yet.
                pass

        if clear_existing:
            os.makedirs(
                self.folder_path, exist_ok=True
            )  # Ensure folder exists for fresh start

            self.drives = []  # Start with an empty drive list
            min_drives = self.raid_configs[self.raid_level]["min_drives"]
            for i in range(min_drives):
                self.add_drive(
                    initial_setup=True
                )  # Add drives with initial setup flag

            self.current_logical_block_index = 0
            self.logical_to_physical_map = {}
            # Generate a new signature for a freshly initialized RAID
            self.raid_signature = f"RAID-{self.raid_level}-{datetime.datetime.now().timestamp()}"
            self._save_config()  # Save the initial configuration
            logger.log(
                f"RAID-{self.raid_level} initialized fresh with {len(self.drives)} drives"
            )
        else:
            # If not clearing, attempt to load the existing configuration.
            # If loading fails, we'll fall back to creating a new one.
            if not self._load_config():
                logger.log(
                    "Failed to load RAID configuration. Creating a new one.", "WARN"
                )
                self.initialize_raid_structure(
                    clear_existing=True
                )  # Recursively call to clear and re-create
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
            # Convert integer keys to strings for JSON, then back when loading
            "logical_to_physical_map": {
                str(lba): {str(d_id): p_sec for d_id, p_sec in d_map.items()}
                for lba, d_map in self.logical_to_physical_map.items()
            },
            "drives": [
                {
                    "id": d.drive_id,
                    "signature": d.signature,
                    "is_active": d.is_active,
                    "status": d.metadata["status"],
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
        """
        Loads RAID configuration from a JSON file.
        Returns True if successful, False otherwise.
        """
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
            # Convert keys back to integers when loading
            self.logical_to_physical_map = {
                int(lba): {int(d_id): p_sec for d_id, p_sec in d_map.items()}
                for lba, d_map in loaded_map.items()
            }

            self.drives = []  # Clear current drives list before populating
            for drive_info in config_data.get("drives", []):
                drive_id = drive_info["id"]
                drive_signature = drive_info["signature"]
                drive_status = drive_info.get("status", "unknown")

                drive_file_path = os.path.join(self.folder_path, f"disk_{drive_id}")

                new_drive = Drive(
                    drive_id, self.folder_path, signature=drive_signature
                )  # Re-create with original signature

                # Check if the drive file actually exists and its signature matches
                if not os.path.exists(drive_file_path):
                    logger.log(
                        f"Drive file {drive_file_path} missing for drive {drive_id}. Marking as failed.",
                        "ERROR",
                    )
                    new_drive.is_active = False
                    new_drive.metadata["status"] = "failed_file_missing"
                    new_drive.sectors = {}  # No data to load if file is missing
                elif (
                    new_drive.signature != drive_signature
                    and drive_status != "failed"
                ):
                    # If an active drive's signature doesn't match, it's inconsistent
                    logger.log(
                        f"Drive {drive_id} signature mismatch. Expected {drive_signature}, found {new_drive.signature}. Marking as failed.",
                        "ERROR",
                    )
                    new_drive.is_active = False
                    new_drive.metadata["status"] = "failed_signature_mismatch"
                else:
                    # Drive file exists and signature matches (or was already failed)
                    new_drive.is_active = drive_info.get("is_active", False)
                    new_drive.metadata["status"] = drive_status
                    # For a loaded drive, we're not actually parsing data from its file here.
                    # We assume it was correctly written and rebuilds would restore data.
                    # This simplified demo only persists metadata to Drive objects.
                    new_drive.next_physical_sector = drive_info.get(
                        "next_physical_sector", 0
                    )
                self.drives.append(new_drive)

            # Keep drives sorted by ID for consistent array indexing
            self.drives.sort(key=lambda d: d.drive_id)
            return True
        except Exception as e:
            logger.log(f"Error loading RAID configuration: {e}", "ERROR")
            return False

    def add_drive(self, initial_setup: bool = False, existing_signature: Optional[str] = None):
        """
        Adds a new drive to the RAID array, assigning it the next available ID.
        If `initial_setup` is True, it's part of the RAID's initial creation.
        If `existing_signature` is provided, it tries to re-add a drive with that signature.
        """
        if self.raid_level in [10, 50, 60] and not initial_setup:
            logger.log(
                f"Adding drives to RAID-{self.raid_level} is more complex and typically requires expanding sub-arrays, which is beyond this demo's scope for dynamic addition. Please re-initialize with more drives if needed.",
                "WARN",
            )
            print(f"\033[93mWARNING: Adding drives dynamically to RAID-{self.raid_level} is not supported in this demo.\033[0m")
            return None

        # Find the next sequential drive_id, accounting for potential gaps
        if not self.drives:
            new_drive_id = 0
        else:
            existing_ids = sorted([d.drive_id for d in self.drives])
            new_drive_id = 0
            for i in existing_ids:
                if i == new_drive_id:
                    new_drive_id += 1
                else:
                    break  # Found a gap, use this ID

        new_drive = Drive(new_drive_id, self.folder_path, signature=existing_signature)
        self.drives.append(new_drive)
        self.drives.sort(key=lambda d: d.drive_id)  # Maintain sorted order
        self._save_config()
        logger.log(f"Added drive {new_drive_id} to RAID-{self.raid_level}")

        if not initial_setup and self.raid_level in [0, 5, 6]:
            print(
                "\n\033[93mNOTICE: For RAID-0, RAID-5, and RAID-6, adding a drive typically requires "
                "rebalancing/rebuilding the entire array to incorporate the new drive "
                "into the striping pattern. This will essentially rebuild the entire index "
                "and redistribute blocks. This demo simulates adding an empty drive, "
                "and future writes will use it. For full rebalancing, a complete rebuild "
                "operation would be needed.\033[0m"
            )
        elif not initial_setup and self.raid_level == 1:
            print(
                "\n\033[93mNOTICE: For RAID-1, adding a drive creates a new mirror. "
                "A rebuild/synchronization process would typically copy all data "
                "from an existing mirror to the new drive to make it a full copy. "
                "This will be automatically initiated for the newly added drive.\033[0m"
            )
            # For RAID-1, trigger a "rebuild" (clone) for the new drive
            self.start_rebuild(
                failed_logical_drive_position=-1,  # Special value for a new drive rebuild
                replacement_drive_id=new_drive_id,
                is_new_drive_add=True
            )

        return new_drive_id

    def remove_drive(self, drive_id_to_fail: int):
        """
        Simulates the failure of a drive. It marks the drive as inactive and
        then prompts the user for a recovery action (re-add, replace/rebuild, or ignore).
        """
        # Validate the drive ID provided by the user
        if not (0 <= drive_id_to_fail < len(self.drives)) or self.drives[
            drive_id_to_fail
        ].drive_id != drive_id_to_fail:
            logger.log(
                f"Drive ID {drive_id_to_fail} not found or index mismatch. Please use a valid ID from display_status.",
                "ERROR",
            )
            return

        failed_drive_obj = self.drives[drive_id_to_fail]

        if not failed_drive_obj.is_active:
            logger.log(
                f"Drive {drive_id_to_fail} is already inactive/failed.", "WARN"
            )
            return  # Don't prompt for rebuild if already failed

        failed_drive_obj.mark_failed()  # Mark the drive as failed
        self._save_config()

        active_drives_count = sum(1 for d in self.drives if d.is_active)
        fault_tolerance = self.raid_configs[self.raid_level]["fault_tolerance"]

        # Check if the RAID array can survive this failure based on its fault tolerance
        if active_drives_count < (len(self.drives) - fault_tolerance):
            logger.log(
                "RAID FAILURE: Not enough drives to maintain data integrity!", "ERROR"
            )
            print("\n\033[91m!!! CRITICAL RAID FAILURE !!! Data may be lost.\033[0m")
            if self.raid_level == 0:
                print(
                    "RAID-0 has no fault tolerance. Data on the logical drive is permanently failed."
                )
                # For RAID-0, mark relevant logical blocks as permanently lost
                for lba_map in self.logical_to_physical_map.values():
                    if drive_id_to_fail in lba_map:
                        lba_map[
                            drive_id_to_fail
                        ] = -1  # Indicate permanently failed block
            return  # Cannot recover beyond fault tolerance, exit

        else:
            logger.log(f"RAID-{self.raid_level} operating in degraded mode", "WARN")
            print(
                f"\n\033[93mRAID is operating in degraded mode. Failed drive: {drive_id_to_fail}\033[0m"
            )

            print("1. Re-add *existing* drive (if it was temporarily removed and is now back)")
            print("2. Add *new replacement* drive and start rebuild")
            print("3. Do nothing for now")

            choice = input("Enter your choice (1-3): ").strip()

            if choice == "1":
                # Option to re-add the *same* physical drive if it was just disconnected
                # This assumes the drive's content wasn't corrupted or wiped.
                if not failed_drive_obj.is_active and os.path.exists(
                    failed_drive_obj.file_path
                ):
                    # For RAID-0, re-adding a failed drive doesn't magically recover data.
                    # The user needs to understand that.
                    if self.raid_level == 0:
                        logger.log(
                            f"Cannot effectively 're-add' a failed drive {drive_id_to_fail} for RAID-0 "
                            "without data loss, as RAID-0 has no redundancy. "
                            "Consider a new setup if data is critical.",
                            "ERROR",
                        )
                        failed_drive_obj.metadata["status"] = "permanently_failed"
                        failed_drive_obj._update_file()
                        return

                    # For redundant RAIDs, if signature matches, we can "re-integrate"
                    # In a real system, this would trigger a resync. Here, we simply
                    # reactivate and then suggest a rebuild (which `start_rebuild` would do).
                    if failed_drive_obj.metadata.get("signature") == failed_drive_obj.signature:
                        failed_drive_obj.is_active = True
                        failed_drive_obj.metadata["status"] = "re_adding" # Temporarily re-adding
                        failed_drive_obj._update_file()
                        logger.log(
                            f"Drive {drive_id_to_fail} being re-added. Signature match: {failed_drive_obj.signature}",
                            "INFO",
                        )
                        self._save_config()
                        # Immediately start a rebuild to resync the data
                        self.start_rebuild(failed_logical_drive_position=drive_id_to_fail, replacement_drive_id=drive_id_to_fail)
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
                # Add a *new* drive and then start the rebuild process
                replacement_id = self.add_drive(
                    initial_setup=False
                )  # Add a fresh drive
                if replacement_id is not None:
                    self.start_rebuild(
                        failed_logical_drive_position=drive_id_to_fail,
                        replacement_drive_id=replacement_id,
                    )

    def write_data(self, data: str):
        """
        Writes the given data to the RAID array according to its configured
        RAID level. This method now breaks the input `data` string into
        individual logical blocks (characters in this demo) and processes each.
        """
        logger.log(f"Writing data: '{data}' to RAID-{self.raid_level}")

        # Basic checks to prevent writes to severely degraded or failed arrays
        active_drives_count = sum(1 for d in self.drives if d.is_active)
        fault_tolerance = self.raid_configs[self.raid_level]["fault_tolerance"]
        min_drives_for_write = (
            len(self.drives) - fault_tolerance
        )  # E.g., for RAID-0, this is len(drives)

        if self.raid_level == 0 and active_drives_count < len(self.drives):
            logger.log(
                "RAID-0 cannot write data with failed drives (no fault tolerance).", "ERROR"
            )
            print(
                "\033[91mCannot write: RAID-0 has no fault tolerance and drives have failed.\033[0m"
            )
            return
        elif (
            self.raid_level in [1, 5, 6, 10, 50, 60]
            and active_drives_count < min_drives_for_write
        ):
            logger.log(
                f"RAID-{self.raid_level} cannot write data due to excessive drive failures. Needs at least {min_drives_for_write} active drives.",
                "ERROR",
            )
            print(
                f"\033[91mCannot write: RAID-{self.raid_level} has too many failed drives. Needs at least {min_drives_for_write} active drives.\033[0m"
            )
            return

        for char_data in data:
            current_lba = self.current_logical_block_index
            self.logical_to_physical_map[current_lba] = {}

            try:
                # Call the specific write method based on RAID level for each character (logical block)
                if self.raid_level == 0:
                    self._write_raid0(char_data, current_lba)
                elif self.raid_level == 1:
                    self._write_raid1(char_data, current_lba)
                elif self.raid_level == 5:
                    self._write_raid5(char_data, current_lba)
                elif self.raid_level == 6:
                    self._write_raid6(char_data, current_lba)
                elif self.raid_level == 10:
                    self._write_raid10(char_data, current_lba)
                elif self.raid_level == 50:
                    self._write_raid50(char_data, current_lba)
                elif self.raid_level == 60:
                    self._write_raid60(char_data, current_lba)

                self.current_logical_block_index += 1
                self._save_config() # Save config after each LBA is written
                logger.log(
                    f"RAID-{self.raid_level} write operation completed for logical block {current_lba} (data: '{char_data}')"
                )
            except Exception as e:
                logger.log(
                    f"RAID-{self.raid_level} write failed for logical block {current_lba} (data: '{char_data}'): {e}",
                    "ERROR",
                )
                # If write fails, remove the incomplete LBA mapping
                if current_lba in self.logical_to_physical_map:
                    del self.logical_to_physical_map[current_lba]
                # Break the loop if an error occurs for one LBA
                break
        logger.log(f"Finished processing input string '{data}'. Next LBA will be {self.current_logical_block_index}")

    def _write_raid0(self, data: str, lba: int):
        """
        Implements RAID-0 striping: data (a single logical block/char) is written to one active drive.
        No redundancy, so any drive failure means data loss.
        """
        active_drives = [d for d in self.drives if d.is_active]
        if not active_drives:
            logger.log("No active drives available", "ERROR")
            raise Exception("No active drives for RAID-0 write")

        # In RAID-0, if any drive is failed, the array cannot write.
        if len(active_drives) < len(self.drives):
            raise Exception(
                "RAID-0 cannot write data with failed drives (no fault tolerance)"
            )

        # Distribute this single logical block (char) to one drive in a round-robin fashion
        drive_to_write = active_drives[lba % len(active_drives)]

        try:
            physical_sector = drive_to_write.write_sector(data, "DATA", lba)
            self.logical_to_physical_map[lba][
                drive_to_write.drive_id
            ] = physical_sector
        except Exception as e:
            logger.log(f"\033[91mError writing to Drive {drive_to_write.drive_id}: {e}\033[0m", "ERROR")
            drive_to_write.mark_failed()
            self.logical_to_physical_map[lba][drive_to_write.drive_id] = -1 # Permanently failed block
            raise
        time.sleep(0.05)

    def _write_raid1(self, data: str, lba: int):
        """
        Implements RAID-1 mirroring: data (a single logical block/char) is written identically to all active drives.
        Provides high redundancy as long as at least one drive remains active.
        """
        active_drives = [d for d in self.drives if d.is_active]
        if not active_drives:
            logger.log("No active drives available", "ERROR")
            raise Exception("No active drives for RAID-1 write")

        successful_writes = 0
        for drive in self.drives: # Write to all configured drives that are active
            if drive.is_active:
                try:
                    physical_sector = drive.write_sector(data, "DATA", lba)
                    self.logical_to_physical_map[lba][
                        drive.drive_id
                    ] = physical_sector
                    successful_writes += 1
                except Exception as e:
                    logger.log(f"\033[91mError writing to Drive {drive.drive_id}: {e}\033[0m", "ERROR")
                    drive.mark_failed()
            time.sleep(0.05)

        if (successful_writes < len(self.drives) - self.raid_configs[self.raid_level]["fault_tolerance"]):
            raise Exception("Not enough drives successfully written for RAID-1 fault tolerance")


    def _write_raid5(self, data: str, lba: int):
        """
        Implements RAID-5 striping with parity: data (a single logical block/char)
        is written to a data drive, and parity is calculated and written to a separate drive.
        """
        active_drives = [d for d in self.drives if d.is_active]
        if len(active_drives) < self.raid_configs[self.raid_level]["min_drives"]:
            logger.log(
                "RAID-5 requires at least 3 active drives to operate.", "ERROR"
            )
            raise Exception("Not enough active drives for RAID-5 write")

        # Determine which drive will hold the parity for this LBA (rotated parity)
        # Use the global LBA to determine parity drive for the entire array (within active subset)
        parity_drive_active_index = lba % len(active_drives)
        parity_drive = active_drives[parity_drive_active_index]

        # Get data drives for this stripe (all active drives except the parity drive)
        data_drives_for_stripe = [
            d for i, d in enumerate(active_drives) if i != parity_drive_active_index
        ]
        if not data_drives_for_stripe:
            raise Exception("Not enough data drives for RAID-5 striping.")
        
        # In RAID-5, for a single LBA, one block is data and one is parity.
        # We need to pick one data drive to write this 'data' to.
        # Simple approach: use a round-robin for data drives (within the current stripe).
        data_drive_for_this_lba = data_drives_for_stripe[lba % len(data_drives_for_stripe)]

        parity_data_chars = data # For a single character block, parity is just that character if XORing with nothing, then XOR with other data later.
                                 # This `_calculate_parity` assumes multiple chars, so here it will treat single char.
                                 # Simplified: Parity is calculated over the actual data written.

        try:
            physical_sector = data_drive_for_this_lba.write_sector(data, "DATA", lba)
            self.logical_to_physical_map[lba][data_drive_for_this_lba.drive_id] = physical_sector
            # parity_data_chars remains just 'data' for now, if only one data block per LBA
        except Exception as e:
            logger.log(f"\033[91mError writing data to Drive {data_drive_for_this_lba.drive_id}: {e}\033[0m", "ERROR")
            data_drive_for_this_lba.mark_failed()
            raise
        time.sleep(0.05)

        parity_char = self._calculate_parity(data) # Parity for this *single* data block

        try:
            physical_sector = parity_drive.write_sector(parity_char, "PARITY", lba)
            self.logical_to_physical_map[lba][parity_drive.drive_id] = physical_sector
            logger.log(
                f"Parity '{parity_char}' calculated and stored on drive {parity_drive.drive_id}"
            )
        except Exception as e:
            logger.log(f"\033[91mError writing parity to Drive {parity_drive.drive_id}: {e}\033[0m", "ERROR")
            parity_drive.mark_failed()
            raise
        time.sleep(0.05)


    def _write_raid6(self, data: str, lba: int):
        """
        Implements RAID-6 striping with dual parity (P and Q parity).
        Data (a single logical block/char) is written to a data drive,
        and two parity blocks are calculated and written to separate drives.
        """
        active_drives = [d for d in self.drives if d.is_active]
        if len(active_drives) < self.raid_configs[self.raid_level]["min_drives"]:
            logger.log(
                "RAID-6 requires at least 4 active drives to operate.", "ERROR"
            )
            raise Exception("Not enough active drives for RAID-6 write")

        # Determine the two parity drives for this LBA, ensuring they are distinct
        parity_drive_1_idx = lba % len(active_drives)
        parity_drive_2_idx = (lba + 1) % len(active_drives)
        if parity_drive_1_idx == parity_drive_2_idx:
            parity_drive_2_idx = (parity_drive_2_idx + 1) % len(active_drives) # Shift to next unique index

        parity_drive_1 = active_drives[parity_drive_1_idx]
        parity_drive_2 = active_drives[parity_drive_2_idx]

        # Get data drives for this stripe (all active drives except the two parity drives)
        data_drives_for_stripe = [
            d
            for i, d in enumerate(active_drives)
            if i not in [parity_drive_1_idx, parity_drive_2_idx]
        ]
        if not data_drives_for_stripe:
            raise Exception("Not enough data drives for RAID-6 striping.")

        # In RAID-6, for a single LBA, one block is data, two are parity.
        data_drive_for_this_lba = data_drives_for_stripe[lba % len(data_drives_for_stripe)]

        # Write data block
        try:
            physical_sector = data_drive_for_this_lba.write_sector(data, "DATA", lba)
            self.logical_to_physical_map[lba][data_drive_for_this_lba.drive_id] = physical_sector
        except Exception as e:
            logger.log(f"\033[91mError writing data to Drive {data_drive_for_this_lba.drive_id}: {e}\033[0m", "ERROR")
            data_drive_for_this_lba.mark_failed()
            raise
        time.sleep(0.05)

        # Calculate P parity (simple XOR) - for a single data block, P is just the data itself
        p_parity_char = self._calculate_parity(data)

        # Calculate Q parity (simplified for demo, typically Reed-Solomon)
        # For a single block, Q parity is derived from that single block's value and its position.
        q_parity_val = ord(data) ^ (lba % 100) # Simplified Q parity example
        q_parity_char = f"Q{q_parity_val % 1000:03d}"[:4]

        # Write P parity
        try:
            physical_sector_p = parity_drive_1.write_sector(p_parity_char, "PARITY-P", lba)
            self.logical_to_physical_map[lba][parity_drive_1.drive_id] = physical_sector_p
            logger.log(
                f"P-Parity '{p_parity_char}' stored on drive {parity_drive_1.drive_id}"
            )
        except Exception as e:
            logger.log(f"\033[91mError writing P-parity to Drive {parity_drive_1.drive_id}: {e}\033[0m", "ERROR")
            parity_drive_1.mark_failed()
            raise
        time.sleep(0.05)

        # Write Q parity
        try:
            physical_sector_q = parity_drive_2.write_sector(q_parity_char, "PARITY-Q", lba)
            self.logical_to_physical_map[lba][parity_drive_2.drive_id] = physical_sector_q
            logger.log(
                f"Q-Parity '{q_parity_char}' stored on drive {parity_drive_2.drive_id}"
            )
        except Exception as e:
            logger.log(f"\033[91mError writing Q-parity to Drive {parity_drive_2.drive_id}: {e}\033[0m", "ERROR")
            parity_drive_2.mark_failed()
            raise
        time.sleep(0.05)

    def _write_raid10(self, data: str, lba: int):
        """
        Implements RAID-10 (RAID 1+0): Data (a single logical block/char) is
        written to a mirrored pair of drives, chosen by striping.
        """
        if (
            len(self.drives) < self.raid_configs[self.raid_level]["min_drives"]
            or len(self.drives) % 2 != 0
        ):
            logger.log(
                "RAID-10 requires an even number of at least 4 drives.", "ERROR"
            )
            raise Exception("Invalid number of drives for RAID-10.")

        # Group drives into mirrored pairs (e.g., [D0,D1], [D2,D3])
        mirrored_pairs: List[List[Drive]] = []
        for i in range(0, len(self.drives), 2):
            pair_drives = [self.drives[i], self.drives[i + 1]]
            # Ensure at least one drive in each pair is active for writing
            if not any(d.is_active for d in pair_drives):
                raise Exception(
                    f"Mirrored pair {i}-{i+1} is completely failed, cannot write."
                )
            mirrored_pairs.append(pair_drives)

        # Stripe this single logical block (char) across the mirrored pairs
        target_pair_index = lba % len(mirrored_pairs)
        target_pair = mirrored_pairs[target_pair_index]

        successful_writes = 0
        for drive in target_pair:
            if drive.is_active:
                try:
                    physical_sector = drive.write_sector(data, "DATA", lba)
                    self.logical_to_physical_map[lba][
                        drive.drive_id
                    ] = physical_sector
                    successful_writes += 1
                except Exception as e:
                    logger.log(f"\033[91mError writing to Drive {drive.drive_id}: {e}\033[0m", "ERROR")
                    drive.mark_failed()
            time.sleep(0.05)

        if successful_writes == 0:
            raise Exception(
                f"Failed to write logical block '{data}' to any drive in target mirrored pair for LBA {lba}."
            )

        logger.log(f"RAID-10: Data '{data}' written to mirrored stripes for LBA {lba}")

    def _write_raid50(self, data: str, lba: int):
        """
        Implements RAID-50 (RAID 5+0): Data (a single logical block/char) is
        striped across multiple RAID-5 sub-arrays.
        """
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
        for i in range(0, len(self.drives), min_drives_for_subarray):
            subarray_drives = self.drives[i : i + min_drives_for_subarray]
            subarrays.append(subarray_drives)

        # Determine which subarray this LBA (data block) will be written to by striping
        target_subarray_index = lba % num_subarrays
        target_subarray = subarrays[target_subarray_index]

        # Simulate a RAID-5 write operation *within* this target subarray for the current LBA
        active_subarray_drives = [d for d in target_subarray if d.is_active]
        if len(active_subarray_drives) < min_drives_for_subarray - self.raid_configs[5]['fault_tolerance']:
            logger.log(
                f"RAID-50 Subarray {target_subarray_index} failed due to too many drive failures. Cannot write data.",
                "ERROR",
            )
            raise Exception(f"RAID-50 Subarray {target_subarray_index} failed, cannot write.")

        # Determine parity drive within this subarray
        parity_drive_subarray_index = lba % len(active_subarray_drives)
        parity_drive = active_subarray_drives[parity_drive_subarray_index]

        data_drives_for_stripe = [
            d
            for idx, d in enumerate(active_subarray_drives)
            if idx != parity_drive_subarray_index
        ]
        
        # Select one data drive within the subarray for this LBA
        data_drive_for_this_lba = data_drives_for_stripe[lba % len(data_drives_for_stripe)]

        # Write data block
        try:
            physical_sector = data_drive_for_this_lba.write_sector(data, "DATA", lba)
            self.logical_to_physical_map[lba][data_drive_for_this_lba.drive_id] = physical_sector
        except Exception as e:
            logger.log(
                f"\033[91mError writing data to Drive {data_drive_for_this_lba.drive_id} in subarray {target_subarray_index}: {e}\033[0m",
                "ERROR",
            )
            data_drive_for_this_lba.mark_failed()
            raise
        time.sleep(0.05)

        parity_char = self._calculate_parity(data) # Parity for this single block

        try:
            physical_sector_parity = parity_drive.write_sector(parity_char, "PARITY", lba)
            self.logical_to_physical_map[lba][parity_drive.drive_id] = physical_sector_parity
            logger.log(
                f"RAID-50 Subarray {target_subarray_index}: Parity '{parity_char}' stored on drive {parity_drive.drive_id}"
            )
        except Exception as e:
            logger.log(
                f"\033[91mError writing parity to Drive {parity_drive.drive_id} in subarray {target_subarray_index}: {e}\033[0m",
                "ERROR",
            )
            parity_drive.mark_failed()
            raise
        time.sleep(0.05)

        logger.log(f"RAID-50: Data '{data}' written to striped RAID-5 subarrays for LBA {lba}")

    def _write_raid60(self, data: str, lba: int):
        """
        Implements RAID-60 (RAID 6+0): Data (a single logical block/char) is
        striped across multiple RAID-6 sub-arrays.
        """
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
        for i in range(0, len(self.drives), min_drives_for_subarray):
            subarray_drives = self.drives[i : i + min_drives_for_subarray]
            subarrays.append(subarray_drives)

        # Determine which subarray this LBA (data block) will be written to by striping
        target_subarray_index = lba % num_subarrays
        target_subarray = subarrays[target_subarray_index]

        # Simulate a RAID-6 write operation *within* this target subarray for the current LBA
        active_subarray_drives = [d for d in target_subarray if d.is_active]
        if len(active_subarray_drives) < min_drives_for_subarray - self.raid_configs[6]['fault_tolerance']:
            logger.log(
                f"RAID-60 Subarray {target_subarray_index} failed due to too many drive failures. Cannot write data.",
                "ERROR",
            )
            raise Exception(f"RAID-60 Subarray {target_subarray_index} failed, cannot write.")

        # Determine parity drives within this subarray
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
                f"RAID-60 Subarray {target_subarray_index}: Not enough data drives for RAID-6 striping."
            )

        # Select one data drive within the subarray for this LBA
        data_drive_for_this_lba = data_drives_for_stripe[lba % len(data_drives_for_stripe)]

        # Write data block
        try:
            physical_sector = data_drive_for_this_lba.write_sector(data, "DATA", lba)
            self.logical_to_physical_map[lba][data_drive_for_this_lba.drive_id] = physical_sector
        except Exception as e:
            logger.log(
                f"\033[91mError writing data to Drive {data_drive_for_this_lba.drive_id} in subarray {target_subarray_index}: {e}\033[0m",
                "ERROR",
            )
            data_drive_for_this_lba.mark_failed()
            raise
        time.sleep(0.05)

        # Calculate P parity
        p_parity_char = self._calculate_parity(data)

        # Calculate Q parity
        q_parity_val = ord(data) ^ (lba % 100)
        q_parity_char = f"Q{q_parity_val % 1000:03d}"[:4]

        # Write P parity
        try:
            physical_sector_p = parity_drive_1.write_sector(p_parity_char, "PARITY-P", lba)
            self.logical_to_physical_map[lba][parity_drive_1.drive_id] = physical_sector_p
            logger.log(
                f"RAID-60 Subarray {target_subarray_index}: P-Parity '{p_parity_char}' stored on drive {parity_drive_1.drive_id}"
            )
        except Exception as e:
            logger.log(
                f"\033[91mError writing P-parity to Drive {parity_drive_1.drive_id} in subarray {target_subarray_index}: {e}\033[0m",
                "ERROR",
            )
            parity_drive_1.mark_failed()
            raise
        time.sleep(0.05)

        # Write Q parity
        try:
            physical_sector_q = parity_drive_2.write_sector(q_parity_char, "PARITY-Q", lba)
            self.logical_to_physical_map[lba][parity_drive_2.drive_id] = physical_sector_q
            logger.log(
                f"RAID-60 Subarray {target_subarray_index}: Q-Parity '{q_parity_char}' stored on drive {parity_drive_2.drive_id}"
            )
        except Exception as e:
            logger.log(
                f"\033[91mError writing Q-parity to Drive {parity_drive_2.drive_id} in subarray {target_subarray_index}: {e}\033[0m",
                "ERROR",
            )
            parity_drive_2.mark_failed()
            raise
        time.sleep(0.05)

        logger.log(f"RAID-60: Data '{data}' written to striped RAID-6 subarrays for LBA {lba}")


    def _calculate_parity(self, data: str) -> str:
        """
        Calculates a simple XOR parity for the given data string.
        This is a basic demonstration; real RAID systems use more complex algorithms.
        """
        if not data:
            return "0000"

        parity_val = 0
        for char in data:
            try:
                parity_val ^= ord(char)  # XOR the ASCII values of characters
            except TypeError:
                logger.log(
                    f"WARNING: Non-character data in parity calculation: '{char}'", "WARN"
                )
                pass

        parity_str = f"P{parity_val:03d}"[:4]  # Format as Pxxx
        return parity_str

    def start_rebuild(
        self,
        failed_logical_drive_position: int,
        replacement_drive_id: int,
        is_new_drive_add: bool = False,
    ):
        """
        Initiates the rebuild process for a drive. This can be for a failed
        drive being replaced or a new drive being added (especially for RAID-1).
        """
        if self.rebuild_active:
            logger.log("REBUILD: Another rebuild is already active.", "WARN")
            print("\033[93mWARNING: Another rebuild is already active. Please wait.\033[0m")
            return

        # Validate drive IDs
        if not (0 <= replacement_drive_id < len(self.drives)):
            logger.log(f"Invalid replacement drive ID: {replacement_drive_id}", "ERROR")
            print("\033[91mERROR: Invalid replacement drive ID.\033[0m")
            return

        replacement_drive_obj = self.drives[replacement_drive_id]

        if not is_new_drive_add:
            # Rebuilding a *failed* drive
            if not (0 <= failed_logical_drive_position < len(self.drives)):
                logger.log(
                    f"Invalid failed logical drive position: {failed_logical_drive_position}",
                    "ERROR",
                )
                print("\033[91mERROR: Invalid failed logical drive position.\033[0m")
                return

            failed_drive_obj = self.drives[failed_logical_drive_position]

            if failed_logical_drive_position == replacement_drive_id:
                # This case implies re-adding the same physical drive after failure
                if failed_drive_obj.is_active:
                    logger.log(
                        f"Drive {failed_logical_drive_position} is active and not marked failed. Cannot rebuild.",
                        "WARN",
                    )
                    print(f"\033[93mWARNING: Drive {failed_logical_drive_position} is active. No rebuild needed.\033[0m")
                    return
                # If it's the same drive, but marked failed, we proceed to rebuild it
            elif replacement_drive_obj.is_active:
                logger.log(
                    f"Replacement drive {replacement_drive_id} is already active.",
                    "WARN",
                )
                print(f"\033[93mWARNING: Replacement drive {replacement_drive_id} is already active. Select a failed or new drive.\033[0m")
                return

            # Clear and prepare the replacement drive if it's truly a *new* replacement
            if failed_logical_drive_position != replacement_drive_id:
                replacement_drive_obj.sectors = {}
                replacement_drive_obj.next_physical_sector = 0

            replacement_drive_obj.is_active = True
            replacement_drive_obj.metadata["status"] = "rebuilding"
            replacement_drive_obj._update_file()
            logger.log(f"Drive {replacement_drive_id} set to 'rebuilding' status.")
        else:
            # Rebuilding a *newly added* drive (e.g., RAID-1 sync)
            if replacement_drive_obj.is_active and replacement_drive_obj.metadata["used_sectors"] > 0:
                 logger.log(f"New drive {replacement_drive_id} already has data, assuming it's consistent for this demo.", "INFO")
                 replacement_drive_obj.metadata["status"] = "active"
                 replacement_drive_obj._update_file()
                 self._save_config()
                 return # No rebuild needed if new drive already has data (e.g. pre-filled)
            
            replacement_drive_obj.sectors = {}
            replacement_drive_obj.next_physical_sector = 0
            replacement_drive_obj.is_active = True
            replacement_drive_obj.metadata["status"] = "syncing"
            replacement_drive_obj._update_file()
            logger.log(f"New drive {replacement_drive_id} set to 'syncing' status for rebuild.")

        self.rebuild_active = True
        self.rebuild_thread = threading.Thread(
            target=self._rebuild_worker,
            args=(
                failed_logical_drive_position,
                replacement_drive_id,
                is_new_drive_add,
            ),
            daemon=True,
        )
        self.rebuild_thread.start()
        logger.log(
            f"Started rebuild: Failed logical position {failed_logical_drive_position} -> Drive {replacement_drive_id}"
        )
        print(f"\n\033[92mRebuild process started for Drive {replacement_drive_id}. Check logs for progress.\033[0m")

    def _rebuild_worker(
        self,
        failed_logical_drive_position: int,
        replacement_drive_id: int,
        is_new_drive_add: bool,
    ):
        """
        The background worker thread that performs the actual data reconstruction
        and writes it to the replacement drive.
        """
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
                print("\033[92mRebuild/Sync completed: No data to transfer.\033[0m")
                return

            for lba in range(total_logical_blocks):
                if not self.rebuild_active:
                    break  # Stop if rebuild is cancelled

                time.sleep(0.05)  # Simulate rebuild effort

                physical_sector_map_for_lba = self.logical_to_physical_map.get(lba, {})

                # Determine the physical sector on the *target* drive (the one being rebuilt/synced)
                # If a drive failed, we look up its old physical sector for this LBA.
                # If a new drive is added, we determine a sector for it based on the LBA.
                # For this demo, we can just use the next available physical sector on the replacement drive for this LBA.
                # Or for simplicity, assume target_physical_sector_on_replacement should exist from original map.
                
                # We need the physical sector that *should have been* or *will be* occupied by this LBA on the target drive
                if not is_new_drive_add:
                    target_physical_sector_on_replacement = physical_sector_map_for_lba.get(failed_logical_drive_position)
                    if target_physical_sector_on_replacement is None: # This LBA wasn't mapped to the failed drive
                        # Assign a new logical physical sector to the replacement drive
                        target_physical_sector_on_replacement = replacement_drive.next_physical_sector
                        replacement_drive.next_physical_sector += 1 # Manually increment since write_to_specific won't for this case
                else: # New drive add
                    target_physical_sector_on_replacement = replacement_drive.next_physical_sector
                    replacement_drive.next_physical_sector += 1


                if target_physical_sector_on_replacement is None:
                    # If this LBA wasn't originally mapped to the failed/new drive's position, it's fine.
                    # Or if it was marked -1 (permanently lost for RAID-0).
                    if (not is_new_drive_add and self.raid_level == 0 and failed_logical_drive_position in physical_sector_map_for_lba and physical_sector_map_for_lba[failed_logical_drive_position] == -1):
                        logger.log(f"REBUILD: LBA {lba} already marked permanently lost for RAID-0 on drive {failed_logical_drive_position}. Skipping.", "INFO")
                    else:
                        logger.log(
                            f"REBUILD WARN: Logical block {lba} did not have a corresponding physical sector on the target logical position, skipping.",
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
                    # For RAID-1, find any active mirror drive and copy its data
                    for drive in self.drives:
                        if drive.is_active and drive.drive_id != replacement_drive_id:
                            mirror_physical_sector = physical_sector_map_for_lba.get(
                                drive.drive_id
                            )
                            if mirror_physical_sector is not None and mirror_physical_sector != -1:
                                data = drive.read_sector(mirror_physical_sector)
                                if data is not None:
                                    rebuilt_data = data
                                    break
                                else:
                                    logger.log(
                                        f"REBUILD WARN: Could not read data from drive {drive.drive_id} for LBA {lba} at physical sector {mirror_physical_sector}",
                                        "WARN",
                                    )
                    if rebuilt_data is None:
                        logger.log(
                            f"REBUILD ERROR: No active mirror found to reconstruct LBA {lba} for RAID-1.",
                            "ERROR",
                        )
                        rebuilt_data = "???"


                elif self.raid_level == 5:
                    # Reconstruct data from remaining data and parity blocks
                    num_total_raid_drives = len(self.drives)
                    
                    # We need to find which drive was meant to hold parity for *this* LBA's stripe
                    # This calculation should be based on original total drives or a stable configuration
                    
                    # For a single data block (LBA), RAID-5 stores data on one drive and parity on another.
                    # We need to determine if the failed/new drive was supposed to hold the data or the parity.

                    # First, identify the data drive and parity drive for this LBA's stripe *conceptually*.
                    # This calculation is simplified for a single-character logical block per LBA.
                    
                    # For simplicity of reconstruction:
                    # Iterate through all *other* active drives for this LBA and see what data/parity they hold.
                    
                    collected_data = []
                    collected_parity = None
                    
                    # Determine which active drive for this LBA held the data
                    data_drive_for_this_lba = None
                    for d_id in physical_sector_map_for_lba:
                        if d_id != replacement_drive_id and d_id < len(self.drives) and self.drives[d_id].is_active:
                            drive = self.drives[d_id]
                            p_sector = physical_sector_map_for_lba[d_id]
                            if p_sector != -1 and p_sector in drive.sectors:
                                sector_info = drive.sectors[p_sector]
                                if sector_info["type"] == "DATA":
                                    collected_data.append(sector_info["data"])
                                    data_drive_for_this_lba = drive.drive_id
                                elif sector_info["type"] == "PARITY":
                                    collected_parity = sector_info["data"]

                    # Now, based on what's available, reconstruct
                    if not is_new_drive_add and failed_logical_drive_position == data_drive_for_this_lba:
                        # The data drive failed, reconstruct data using the parity
                        if collected_parity:
                            # In this single-block-per-LBA demo, parity is derived directly from the data.
                            # So if the data block is 'X' and parity is 'P_X', then the missing data is 'X'.
                            # More realistically, you'd XOR collected_data and collected_parity.
                            rebuilt_data = chr(int(collected_parity[1:]) % 128) # Simple reverse from PXXX
                        else:
                            rebuilt_data = "???" # Cannot reconstruct
                    elif not is_new_drive_add and failed_logical_drive_position != data_drive_for_this_lba and collected_data:
                        # The parity drive failed or a new drive is added, recalculate parity
                        rebuilt_data = self._calculate_parity(collected_data[0] if collected_data else "")
                    elif is_new_drive_add and collected_data: # New drive, needs the data or parity
                        # Determine if the new drive *should* hold data or parity for this LBA
                        num_total_raid_drives = len(self.drives) # Now includes the new drive
                        active_drives_now = [d for d in self.drives if d.is_active] # Includes new drive
                        parity_drive_active_index_now = lba % len(active_drives_now)
                        
                        if replacement_drive_id == active_drives_now[parity_drive_active_index_now].drive_id:
                            # New drive is meant to hold parity
                            rebuilt_data = self._calculate_parity(collected_data[0] if collected_data else "")
                        else:
                            # New drive is meant to hold data
                            if collected_parity and collected_data:
                                rebuilt_data = collected_data[0] # Copy data from an existing mirror/source
                            elif collected_parity:
                                # Reconstruct data from parity if no direct data source (more complex for single char)
                                rebuilt_data = chr(int(collected_parity[1:]) % 128)
                            else:
                                rebuilt_data = "???"

                    if rebuilt_data is None:
                        logger.log(
                            f"REBUILD WARN: No sufficient data/parity to reconstruct LBA {lba} for RAID-5. Assuming data lost.",
                            "WARN",
                        )
                        rebuilt_data = "???"


                elif self.raid_level == 6:
                    # Reconstruct data using P and Q parity, handling up to two failures
                    # Similar logic to RAID-5 reconstruction, but with two parity blocks.
                    # Simplified for this demo (typically requires Galois Field math).
                    
                    collected_data = []
                    collected_p_parity = None
                    collected_q_parity = None

                    # Collect available data/parity from active drives
                    for d_id in physical_sector_map_for_lba:
                        if d_id != replacement_drive_id and d_id < len(self.drives) and self.drives[d_id].is_active:
                            drive = self.drives[d_id]
                            p_sector = physical_sector_map_for_lba[d_id]
                            if p_sector != -1 and p_sector in drive.sectors:
                                sector_info = drive.sectors[p_sector]
                                if sector_info["type"] == "DATA":
                                    collected_data.append(sector_info["data"])
                                elif sector_info["type"] == "PARITY-P":
                                    collected_p_parity = sector_info["data"]
                                elif sector_info["type"] == "PARITY-Q":
                                    collected_q_parity = sector_info["data"]

                    # Determine if the target drive (failed_logical_drive_position or replacement_drive_id)
                    # was supposed to hold data, P parity, or Q parity.
                    
                    # For single character logical block, reconstruction is simplified:
                    # If data is missing, and we have P, we can get data back (if only one failure).
                    # If P is missing, recalculate from data.
                    # If Q is missing, recalculate from data.
                    
                    if not is_new_drive_add and collected_data: # If we have data, we can recalculate parity
                        if collected_p_parity is None: # P parity missing
                            rebuilt_data = self._calculate_parity(collected_data[0])
                        elif collected_q_parity is None: # Q parity missing
                            q_recalc_val = ord(collected_data[0]) ^ (lba % 100)
                            rebuilt_data = f"Q{q_recalc_val % 1000:03d}"[:4]
                        else: # Data missing
                            # With P and Q, and all other data blocks, we can reconstruct the missing data block.
                            # In this simplified demo, assuming one data block and its P & Q parity
                            # If only 1 data block, P is data, Q is data XOR LBA info.
                            rebuilt_data = collected_p_parity[0] if collected_p_parity and collected_p_parity.startswith('P') else (collected_q_parity[0] if collected_q_parity and collected_q_parity.startswith('Q') else "???")
                            if rebuilt_data.startswith('P'): rebuilt_data = chr(int(rebuilt_data[1:]) % 128) # If data was hidden in P
                            if rebuilt_data.startswith('Q'): rebuilt_data = chr(int(rebuilt_data[1:]) % 128) # If data was hidden in Q
                    elif is_new_drive_add and collected_data: # New drive add
                        # For a new drive, it needs to get its data or parity.
                        # This requires knowing if the new drive is a data, P, or Q drive for this LBA.
                        # This is complex in simplified RAID-6, assume it gets copied if it's a data drive.
                        rebuilt_data = collected_data[0] # Simplistically, if data available, copy.
                    else:
                        logger.log(
                            f"REBUILD WARN: No sufficient data/parity to reconstruct LBA {lba} for RAID-6. Assuming data lost.",
                            "WARN",
                        )
                        rebuilt_data = "???"

                elif self.raid_level == 10:
                    # For RAID 10, data reconstruction is local to the mirrored pair.
                    # Find the mirrored pair that the failed drive belongs to.
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
                            # The other active drive in the pair holds the mirror copy
                            if drive.is_active and drive.drive_id != replacement_drive_id:
                                mirror_physical_sector = physical_sector_map_for_lba.get(
                                    drive.drive_id
                                )
                                if mirror_physical_sector is not None and mirror_physical_sector != -1:
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
                    # RAID-50 rebuild involves performing a RAID-5 rebuild within the affected sub-array.
                    min_drives_for_subarray = self.raid_configs[5]["min_drives"]
                    subarray_index = failed_logical_drive_position // min_drives_for_subarray
                    
                    subarray_start_id = subarray_index * min_drives_for_subarray
                    subarray_drives_global_ids = list(range(subarray_start_id, subarray_start_id + min_drives_for_subarray))

                    active_subarray_drives = [d for d in self.drives if d.drive_id in subarray_drives_global_ids and d.is_active]
                    
                    if not active_subarray_drives:
                        logger.log(f"REBUILD ERROR: RAID-50 Subarray {subarray_index} completely failed, cannot reconstruct LBA {lba}.", "ERROR")
                        rebuilt_data = "???"
                    else:
                        # Similar RAID-5 reconstruction logic adapted for subarray
                        collected_data = []
                        collected_parity = None
                        
                        data_drive_for_this_lba = None
                        for d_id in physical_sector_map_for_lba:
                            if d_id in subarray_drives_global_ids and d_id != replacement_drive_id and d_id < len(self.drives) and self.drives[d_id].is_active:
                                drive = self.drives[d_id]
                                p_sector = physical_sector_map_for_lba[d_id]
                                if p_sector != -1 and p_sector in drive.sectors:
                                    sector_info = drive.sectors[p_sector]
                                    if sector_info["type"] == "DATA":
                                        collected_data.append(sector_info["data"])
                                        data_drive_for_this_lba = drive.drive_id
                                    elif sector_info["type"] == "PARITY":
                                        collected_parity = sector_info["data"]

                        if not is_new_drive_add and failed_logical_drive_position == data_drive_for_this_lba:
                            if collected_parity: rebuilt_data = chr(int(collected_parity[1:]) % 128)
                            else: rebuilt_data = "???"
                        elif not is_new_drive_add and failed_logical_drive_position != data_drive_for_this_lba and collected_data:
                            rebuilt_data = self._calculate_parity(collected_data[0] if collected_data else "")
                        elif is_new_drive_add and collected_data:
                            rebuilt_data = collected_data[0]
                        else:
                            rebuilt_data = "???"

                    if rebuilt_data is None:
                        logger.log(f"REBUILD WARN: RAID-50 Subarray {subarray_index}: No sufficient data/parity to reconstruct LBA {lba}. Assuming data lost.", "WARN")
                        rebuilt_data = "???"

                elif self.raid_level == 60:
                    # RAID-60 rebuild involves performing a RAID-6 rebuild within the affected sub-array.
                    min_drives_for_subarray = self.raid_configs[6]["min_drives"]
                    subarray_index = failed_logical_drive_position // min_drives_for_subarray

                    subarray_start_id = subarray_index * min_drives_for_subarray
                    subarray_drives_global_ids = list(range(subarray_start_id, subarray_start_id + min_drives_for_subarray))

                    active_subarray_drives = [d for d in self.drives if d.drive_id in subarray_drives_global_ids and d.is_active]

                    if not active_subarray_drives:
                        logger.log(f"REBUILD ERROR: RAID-60 Subarray {subarray_index} completely failed, cannot reconstruct LBA {lba}.", "ERROR")
                        rebuilt_data = "???"
                    else:
                        # Similar RAID-6 reconstruction logic adapted for subarray
                        collected_data = []
                        collected_p_parity = None
                        collected_q_parity = None

                        for d_id in physical_sector_map_for_lba:
                            if d_id in subarray_drives_global_ids and d_id != replacement_drive_id and d_id < len(self.drives) and self.drives[d_id].is_active:
                                drive = self.drives[d_id]
                                p_sector = physical_sector_map_for_lba[d_id]
                                if p_sector != -1 and p_sector in drive.sectors:
                                    sector_info = drive.sectors[p_sector]
                                    if sector_info["type"] == "DATA":
                                        collected_data.append(sector_info["data"])
                                    elif sector_info["type"] == "PARITY-P":
                                        collected_p_parity = sector_info["data"]
                                    elif sector_info["type"] == "PARITY-Q":
                                        collected_q_parity = sector_info["data"]
                        
                        if not is_new_drive_add and collected_data:
                            if collected_p_parity is None: rebuilt_data = self._calculate_parity(collected_data[0])
                            elif collected_q_parity is None:
                                q_recalc_val = ord(collected_data[0]) ^ (lba % 100)
                                rebuilt_data = f"Q{q_recalc_val % 1000:03d}"[:4]
                            else: rebuilt_data = collected_data[0]
                        elif is_new_drive_add and collected_data:
                            rebuilt_data = collected_data[0]
                        else:
                            rebuilt_data = "???"

                    if rebuilt_data is None:
                        logger.log(f"REBUILD WARN: RAID-60 Subarray {subarray_index}: No sufficient data/parity to reconstruct LBA {lba}. Assuming data lost.", "WARN")
                        rebuilt_data = "???"


                if rebuilt_data is not None:
                    # Write the reconstructed data to the replacement drive
                    replacement_drive.write_to_specific_sector(
                        target_physical_sector_on_replacement, rebuilt_data, "REBUILT" if not is_new_drive_add else "SYNCED", lba
                    )
                    # Update the logical-to-physical map to point to the new drive's sector
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
            print("\033[92mRebuild/Sync completed successfully!\033[0m")

        except Exception as e:
            logger.log(f"REBUILD: Error during rebuild - {e}", "ERROR")
            print(f"\033[91mERROR: Rebuild failed due to an unexpected error: {e}\033[0m")

        finally:
            self.rebuild_active = False
            self.health_check()  # Run a health check after rebuild

    def health_check(self):
        """
        Performs a comprehensive health check on the RAID array.
        It assesses drive statuses, rebuild progress, and data consistency.
        """
        print(f"\n{'='*60}")
        print("RAID HEALTH CHECK")
        print(f"{'='*60}")
        health_status = "OK"

        # 1. Check active drive count vs fault tolerance
        active_drives_count = sum(1 for d in self.drives if d.is_active)
        fault_tolerance = self.raid_configs[self.raid_level]["fault_tolerance"]

        if active_drives_count < (len(self.drives) - fault_tolerance):
            print("\033[91mSTATUS: CRITICAL - RAID has failed beyond fault tolerance.\033[0m")
            health_status = "CRITICAL"
        elif active_drives_count < len(self.drives):
            print("\033[93mSTATUS: DEGRADED - One or more drives have failed. Rebuild recommended.\033[0m")
            health_status = "DEGRADED (Rebuild Recommended)"
        else:
            print("\033[92mSTATUS: HEALTHY - All configured drives are active.\033[0m")

        # 2. Check rebuild status
        if self.rebuild_active:
            print("\033[93mREBUILD: In progress.\033[0m")
            health_status = "DEGRADED (Rebuild In Progress)"
        elif "DEGRADED" in health_status and not self.rebuild_active:
            print("\033[93mREBUILD: Recommended - No rebuild is currently active despite degraded state.\033[0m")

        # 3. Drive-specific status checks (like signature mismatches, missing files)
        for drive in self.drives:
            if drive.metadata.get("status") == "failed_signature_mismatch":
                print(f"\033[91mDRIVE {drive.drive_id}: FAILED (Signature Mismatch) - Replacement/Rebuild required.\033[0m")
                health_status = "DEGRADED (Signature Mismatch)"
            elif not drive.is_active and "failed" not in drive.metadata.get("status", ""):
                print(f"\033[93mDRIVE {drive.drive_id}: INACTIVE - Status: {drive.metadata['status']}\033[0m")
                health_status = "DEGRADED (Inactive Drive)"
            elif not os.path.exists(drive.file_path) and drive.is_active:
                print(f"\033[91mDRIVE {drive.drive_id}: CRITICAL - Drive file missing but marked active! Marking as failed.\033[0m")
                drive.mark_failed()  # Immediately mark as failed
                health_status = "CRITICAL"

        # 4. Data Consistency/Block Mapping Check - This is a conceptual check
        print("\nDATA CONSISTENCY CHECK:")
        inconsistent_blocks = []
        for lba in range(self.current_logical_block_index):
            mapped_drives_for_lba = self.logical_to_physical_map.get(lba, {})

            actual_data_present = 0
            for drive_id, p_sector in mapped_drives_for_lba.items():
                if drive_id < len(self.drives):
                    drive_obj = self.drives[drive_id]
                    if drive_obj.is_active and p_sector != -1:
                        if p_sector in drive_obj.sectors:
                            actual_data_present += 1
                        # else: # Mapped, active, but sector not found in drive's internal sectors - This should be caught by earlier checks
                            # logger.log(f"LBA {lba}, Drive {drive_id}: Mapped physical sector {p_sector} not found on active drive's internal sectors.", "WARN")
                    elif p_sector == -1:
                        logger.log(
                            f"LBA {lba}, Drive {drive_id}: Marked as permanently lost block.", "INFO"
                        )

            # Logic to determine data integrity based on RAID level
            num_total_stripe_members = len(self.drives)
            if self.raid_level == 0:
                if actual_data_present < num_total_stripe_members:
                    inconsistent_blocks.append(
                        f"LBA {lba} (RAID-0: CRITICAL data loss, {num_total_stripe_members - actual_data_present} parts missing)"
                    )
                    if health_status != "CRITICAL": health_status = "CRITICAL" # Promote to critical if not already

            elif self.raid_level == 1:
                if actual_data_present == 0:
                    inconsistent_blocks.append(
                        f"LBA {lba} (RAID-1: CRITICAL data loss, no active copies remain)"
                    )
                    if health_status != "CRITICAL": health_status = "CRITICAL"
                elif actual_data_present < num_total_stripe_members:
                    inconsistent_blocks.append(
                        f"LBA {lba} (RAID-1: DEGRADED, {num_total_stripe_members - actual_data_present} mirror copies missing)"
                    )
                    if health_status == "OK": health_status = "INCONSISTENT"

            elif self.raid_level == 5:
                # Need (N-1) drives to reconstruct. N is num_total_stripe_members.
                # So we need at least (num_total_stripe_members - 1) active data sources.
                if actual_data_present < (num_total_stripe_members - self.raid_configs[5]['fault_tolerance']):
                    inconsistent_blocks.append(f"LBA {lba} (RAID-5: CRITICAL data loss, {num_total_stripe_members - actual_data_present} sources missing. Beyond fault tolerance.)")
                    if health_status != "CRITICAL": health_status = "CRITICAL"
                elif actual_data_present < num_total_stripe_members:
                    inconsistent_blocks.append(f"LBA {lba} (RAID-5: Degraded, {num_total_stripe_members - actual_data_present} sources missing. Rebuildable.)")
                    if health_status == "OK": health_status = "INCONSISTENT"

            elif self.raid_level == 6:
                # Need (N-2) drives to reconstruct.
                if actual_data_present < (num_total_stripe_members - self.raid_configs[6]['fault_tolerance']):
                    inconsistent_blocks.append(f"LBA {lba} (RAID-6: CRITICAL data loss, {num_total_stripe_members - actual_data_present} sources missing. Beyond fault tolerance.)")
                    if health_status != "CRITICAL": health_status = "CRITICAL"
                elif actual_data_present < num_total_stripe_members:
                    inconsistent_blocks.append(f"LBA {lba} (RAID-6: Degraded, {num_total_stripe_members - actual_data_present} sources missing. Rebuildable.)")
                    if health_status == "OK": health_status = "INCONSISTENT"
            
            elif self.raid_level == 10:
                num_mirrored_pairs = len(self.drives) // 2
                pairs_with_critical_data_loss = 0
                pairs_with_degraded_status = 0

                for i in range(num_mirrored_pairs):
                    pair_drives_global_ids = [self.drives[2*i].drive_id, self.drives[2*i+1].drive_id]
                    active_sources_in_pair = 0
                    for d_id in pair_drives_global_ids:
                        if d_id in mapped_drives_for_lba and self.drives[d_id].is_active and mapped_drives_for_lba[d_id] != -1:
                            if mapped_drives_for_lba[d_id] in self.drives[d_id].sectors:
                                active_sources_in_pair += 1
                    
                    if active_sources_in_pair == 0:
                        pairs_with_critical_data_loss += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-10 Mirrored Pair {pair_drives_global_ids}: CRITICAL data loss, both drives in pair failed.)")
                    elif active_sources_in_pair == 1:
                        pairs_with_degraded_status += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-10 Mirrored Pair {pair_drives_global_ids}: Degraded, one drive in pair failed. Rebuildable.)")
                
                if pairs_with_critical_data_loss > 0:
                    if health_status != "CRITICAL": health_status = "CRITICAL"
                elif pairs_with_degraded_status > 0 and health_status == "OK":
                    health_status = "INCONSISTENT"

            elif self.raid_level == 50:
                min_drives_for_subarray = self.raid_configs[5]["min_drives"]
                subarray_fault_tolerance = self.raid_configs[5]["fault_tolerance"]
                num_subarrays = len(self.drives) // min_drives_for_subarray
                
                critical_subarrays_for_lba = 0
                degraded_subarrays_for_lba = 0

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
                        critical_subarrays_for_lba += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-50 Subarray {i}: CRITICAL data loss. Beyond fault tolerance.)")
                    elif active_data_sources_in_subarray < min_drives_for_subarray:
                        degraded_subarrays_for_lba += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-50 Subarray {i}: Degraded. Rebuildable.)")
                
                if critical_subarrays_for_lba > 0:
                    if health_status != "CRITICAL": health_status = "CRITICAL"
                elif degraded_subarrays_for_lba > 0 and health_status == "OK":
                    health_status = "INCONSISTENT"

            elif self.raid_level == 60:
                min_drives_for_subarray = self.raid_configs[6]["min_drives"]
                subarray_fault_tolerance = self.raid_configs[6]["fault_tolerance"]
                num_subarrays = len(self.drives) // min_drives_for_subarray
                
                critical_subarrays_for_lba = 0
                degraded_subarrays_for_lba = 0

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
                        critical_subarrays_for_lba += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-60 Subarray {i}: CRITICAL data loss. Beyond fault tolerance.)")
                    elif active_data_sources_in_subarray < min_drives_for_subarray:
                        degraded_subarrays_for_lba += 1
                        inconsistent_blocks.append(f"LBA {lba} (RAID-60 Subarray {i}: Degraded. Rebuildable.)")
                
                if critical_subarrays_for_lba > 0:
                    if health_status != "CRITICAL": health_status = "CRITICAL"
                elif degraded_subarrays_for_lba > 0 and health_status == "OK":
                    health_status = "INCONSISTENT"


        if inconsistent_blocks:
            print("\n\033[91mINCONSISTENT BLOCKS DETECTED:\033[0m")
            for block_info in inconsistent_blocks:
                print(f"- \033[91m{block_info}\033[0m")
            print("\033[93mREBUILD RECOMMENDED to restore consistency if degraded, or CRITICAL data loss suspected.\033[0m")
        else:
            print("All mapped logical blocks appear to be present on active drives.")

        print(f"\nOVERALL HEALTH: \033[1m{health_status}\033[0m")
        print(f"{'='*60}")
        return health_status

    def display_status(self):
        """
        Displays the current status of the RAID array, including drive states
        and logical-to-physical block mappings. Also triggers a health check.
        """
        print(f"\n{'='*60}")
        print(f"RAID-{self.raid_level} STATUS")
        print(f"{'='*60}")
        print(f"Configuration: {self.raid_configs[self.raid_level]['name']}")
        print(f"RAID Signature: {self.raid_signature}")
        print(f"Total Configured Drives: {len(self.drives)}")
        print(f"Active Drives: {sum(1 for d in self.drives if d.is_active)}")
        print(f"Failed Drives: {sum(1 for d in self.drives if not d.is_active)}")
        print(f"Current Logical Block Index: {self.current_logical_block_index}")
        print(f"Rebuild Active: {'Yes' if self.rebuild_active else 'No'}")
        print()

        for drive in self.drives:
            status = drive.metadata["status"]
            # Color-code status for better visibility
            status_display = status
            if "failed" in status:
                status_display = f"\033[91m{status}\033[0m"
            elif "rebuilding" in status or "syncing" in status or "re_adding" in status:
                status_display = f"\033[93m{status}\033[0m"
            else:
                status_display = f"\033[92m{status}\033[0m"

            print(
                f"Drive {drive.drive_id}: {status_display} - {drive.next_physical_sector} physical sectors written (Signature: {drive.signature})"
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

        # Run health check
        self.health_check()

    def cleanup(self):
        """
        Cleans up resources, particularly stopping any active rebuild threads
        to ensure a clean shutdown.
        """
        self.rebuild_active = False
        if self.rebuild_thread and self.rebuild_thread.is_alive():
            logger.log("Waiting for rebuild thread to finish...", "INFO")
            self.rebuild_thread.join(timeout=2.0)
            if self.rebuild_thread.is_alive():
                logger.log("Rebuild thread did not terminate gracefully.", "WARN")


# ---------------------------------------------------------------------
# Interactive mode implementation
# ---------------------------------------------------------------------


def interactive_mode(raid: RAIDArray):
    """
    Provides an interactive menu for users to perform various RAID operations
    like writing data, simulating failures, adding drives, and checking status.
    """
    while True:
        print(f"\n{'='*50}")
        print(f"RAID-{raid.raid_level} Interactive Demo")
        print(f"{'='*50}")
        print("Options:")
        print("1. Write data to RAID")
        print("2. Simulate drive failure (and explore recovery options)")
        print("3. Add new drive to RAID")
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
                print("\033[91mInvalid drive ID (must be a number).\033[0m")
            print("Press Enter to continue...")
            input()

        elif choice == "3":
            # Prevent adding drives for complex RAID levels dynamically in demo
            if raid.raid_level in [10, 50, 60]:
                print(f"\033[93mAdding drives dynamically to RAID-{raid.raid_level} is not supported in this demo. "
                      "Please re-initialize the RAID array with more drives if you wish to expand.\033[0m")
                print("Press Enter to continue...")
                input()
                continue

            print("\nAdd Drive Options:")
            print("a. Add a brand NEW, empty drive")
            print("b. Attempt to re-add an EXISTING drive (e.g., if it was temporarily disconnected but still has its original data/signature)")
            add_choice = input("Enter your choice (a/b): ").strip().lower()

            if add_choice == 'a':
                new_drive_id = raid.add_drive(initial_setup=False)
                if new_drive_id is not None:
                    print(
                        f"Added new drive {new_drive_id}. For RAID-0/5/6, future writes will distribute to it. "
                        "For RAID-1, it will be automatically synced. "
                        "For a full rebalance (0/5/6), a manual rebuild/data migration would typically be needed "
                        "which is beyond this demo's scope for dynamic addition."
                    )
            elif add_choice == 'b':
                try:
                    drive_id_to_readd = int(input("Enter the ID of the existing drive you want to re-add: "))
                    # Check if a drive with this ID already exists but is marked failed
                    existing_drive_match = next((d for d in raid.drives if d.drive_id == drive_id_to_readd), None)

                    if existing_drive_match and not existing_drive_match.is_active:
                        print(f"Attempting to re-add drive {drive_id_to_readd} with its existing signature ({existing_drive_match.signature}).")
                        # This simulates the user physically re-inserting the same drive.
                        # We would then typically initiate a rebuild/resync if needed.
                        if raid.raid_level == 0:
                            print("\033[91mRAID-0: Re-adding a failed drive will not recover lost data. Data blocks associated with this drive for previously written LBAs remain lost.\033[0m")
                            existing_drive_match.is_active = True
                            existing_drive_match.metadata["status"] = "active"
                            existing_drive_match._update_file()
                            raid._save_config()
                            logger.log(f"RAID-0 Drive {drive_id_to_readd} re-activated (data not recovered).", "INFO")
                        else: # For redundant RAID levels, attempt to resync
                            existing_drive_match.is_active = True
                            existing_drive_match.metadata["status"] = "re_adding" # Temporary status
                            existing_drive_match._update_file()
                            raid._save_config()
                            raid.start_rebuild(failed_logical_drive_position=drive_id_to_readd, replacement_drive_id=drive_id_to_readd)
                    elif existing_drive_match and existing_drive_match.is_active:
                        print(f"\033[93mDrive {drive_id_to_readd} is already active. No need to re-add.\033[0m")
                    else:
                        print(f"\033[91mDrive {drive_id_to_readd} not found or doesn't exist in a failed state.\033[0m")
                except ValueError:
                    print("\033[91mInvalid drive ID (must be a number).\033[0m")
            else:
                print("\033[91mInvalid choice for adding drive.\033[0m")

            print("Press Enter to continue...")
            input()

        elif choice == "4":
            raid.display_status()
            print("\nDrive files and raid_config.json created in folder:", raid.folder_path)
            print("You can view these files to see detailed block layouts and RAID state.")
            print("Press Enter to continue...")
            input()

        elif choice == "5":
            print(
                "\n\033[91mWARNING: This will DELETE the current RAID configuration and all simulated drive data!\033[0m"
            )
            confirm = (
                input("Are you sure you want to clear the configuration? (y/n): ")
                .strip()
                .lower()
            )
            if confirm == "y":
                raid.cleanup()  # Clean up current RAID threads gracefully
                logger.log(
                    f"Initiating clear of RAID configuration {raid.folder_path}.",
                    "INFO",
                )
                return "clear_config"  # Special return to trigger re-initialization in main()
            else:
                print("Clear configuration cancelled.")
            print("Press Enter to continue...")
            input()

        elif choice == "6":
            break

        else:
            print("\033[91mInvalid choice. Please try again.\033[0m")
    return None


# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------


def main():
    """
    Main program entry point. Initializes logging, allows selection of RAID level,
    and runs the interactive mode. Handles program shutdown gracefully.
    """
    global logging_active
    global logger
    logger = Logger()  # Initialize the global logger

    print("\n" + "=" * 40)
    print("      RAID Array Simulator with Visualization      ")
    print("=" * 40)
    logger.log("RAIDVIZ application started.")

    try:
        while True:
            print("\nSelect a RAID Level to Demonstrate:")
            print("0 - RAID-0 (Striping) - Performance, No Redundancy")
            print("1 - RAID-1 (Mirroring) - Redundancy, No Performance Gain")
            print("5 - RAID-5 (Striping with Parity) - Performance & Redundancy, 1 Drive Failure")
            print("6 - RAID-6 (Striping with Dual Parity) - Higher Redundancy, 2 Drive Failures")
            print("10 - RAID-10 (Mirrored Stripes) - Performance & High Redundancy")
            print("50 - RAID-50 (Striped RAID-5) - Performance & High Redundancy (Multiple RAID-5s)")
            print("60 - RAID-60 (Striped RAID-6) - Extreme Performance & High Redundancy (Multiple RAID-6s)")
            print("q - Quit")

            choice = (
                input("\nEnter your choice (0, 1, 5, 6, 10, 50, 60, or q): ")
                .strip()
                .lower()
            )

            if choice == "q":
                break

            try:
                raid_level = int(choice)
                if raid_level in [0, 1, 5, 6, 10, 50, 60]:
                    raid = RAIDArray(raid_level)  # Create the RAID array instance

                    raid_folder = f"raid_{raid_level}"
                    config_exists = os.path.exists(raid.config_file)

                    clear_on_next_init = True
                    if config_exists:
                        print(f"\nAn existing RAID-{raid_level} configuration was found in the '{raid_folder}' directory.")
                        prompt = input(
                            "Do you want to wipe it clean and start fresh? (y/n, default 'n' will attempt to load it): "
                        ).strip().lower()
                        if prompt == "y":
                            clear_on_next_init = True
                            # If the user explicitly wants to clear, remove the folder now
                            if os.path.exists(raid_folder):
                                shutil.rmtree(raid_folder)
                                logger.log(f"User chose to clear: Removed {raid_folder}", "INFO")
                        else:
                            clear_on_next_init = False  # Attempt to load existing config

                    # Initialize the RAID structure based on user's choice
                    raid.initialize_raid_structure(clear_existing=clear_on_next_init)

                    # Enter the interactive mode for the chosen RAID
                    result = interactive_mode(raid)
                    if result == "clear_config":
                        raid.cleanup()  # Ensure cleanup before restarting loop
                        continue  # Go back to RAID level selection

                    raid.cleanup()  # Cleanup when exiting interactive mode
                else:
                    print("\033[91mInvalid RAID level selected. Please choose from the available options.\033[0m")
            except ValueError:
                print("\033[91mInvalid input. Please enter a number for the RAID level or 'q' to quit.\033[0m")

    except KeyboardInterrupt:
        print("\n\nUser interrupted. Shutting down gracefully...")

    finally:
        logging_active = False  # Signal the logging thread to stop
        logger.log("RAIDVIZ application shutting down.", "INFO")
        time.sleep(0.5)  # Give the logging thread a moment to process remaining messages


if __name__ == "__main__":
    main()