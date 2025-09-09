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
