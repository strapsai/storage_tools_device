# Device utility functions

import exifread 
import ffmpeg 
import hashlib
import mcap
import mcap.exceptions
import os
import pathlib
import psutil
import pytz 
import queue
import re
import socket
import socketio
import time

from datetime import datetime, timedelta,timezone
from datetime import datetime, timezone
from mcap.reader import make_reader
from queue import Queue
from rosbags.highlevel import AnyReader, AnyReaderError
from typing import List, Tuple

from device.debug_print import debug_print
from device.SocketIOTQDM import MultiTargetSocketIOTQDM
    

class PosMaker:
    """
    Manages positions with a maximum limit, providing the next available position and releasing positions when no longer in use.
    """

    def __init__(self, max_pos) -> None:
        """
        Initializes the PosMaker with a maximum number of positions.
        
        Args:
            max_pos (int): The maximum number of positions available initially.
        """
        self.m_pos = {i: False for i in range(max_pos)}
        self.m_max = max_pos

    def get_next_pos(self) -> int:
        """
        Retrieves the next available position. If all initial positions are in use, it extends the range by one.

        Returns:
            int: The next available position.
        """
        for i in sorted(self.m_pos):
            if not self.m_pos[i]:
                self.m_pos[i] = True
                return i

        # just in case things get messed up, always return a valid position
        i = self.m_max
        self.m_max += 1
        self.m_pos[i] = True
        return i

    def release_pos(self, i):
        """
        Releases a position, making it available again.
        
        Args:
            i (int): The position to release.
        """
        self.m_pos[i] = False


def pbar_thread(messages:Queue, total_size:str, source:str, socket_events:List[Tuple[socketio.Client, str, str]], desc:str, max_threads:int):
    """Multithreaded multitarget nested websocket process bars for data transfer

    This will always create a minimum of two progres bars, one for the main and at least one child.

    command message are dict of [close, main_pbar, child_pbar] -> arg
    * close. args: Argument ignored.  Close main and all child pbars. Exits the loop
    * main_pbar. args: update_value.  Updates the main pbar by this amount.  
    * child_pbar. args: {child_pbar: unique name, action: {[start, update, close] -> dict}}
       * start -> {desc: descriptions of this pbar, size: size of this pbar}. Create a new child pbar.
       * update -> {size: update the pbar by this amount}
       * close. Closes this pbar. 

    Closing the main pbar will close it and all child pbars, even if they are not completed. 
    Closing the main pbar will also exit the function.  

    Args:
        messages (Queue): Command message queue
        total_size (str): Total number of bytes to transfer
        source (str): Source name
        socket_events (List[Tuple[socketio.Client, str, str]]): List of (websocket, event, room|None)
        desc (str): description for main pbar
        max_threads (int): max number of expected concurrent progress bars.  
    """
    pos_maker = PosMaker(max_threads)

    positions = {}

    pbars = {}
    pbars["main_pbar"] = MultiTargetSocketIOTQDM(total=total_size, unit="B", unit_scale=True, leave=False, position=0, delay=1, desc=desc, source=source,socket_events=socket_events)

    while True:
        try:
            action_msg = messages.get(block=True)

        except queue.Empty:
            time.sleep(0.001)
            continue
        except ValueError:
            time.sleep(0.001)
            continue
        
        if "close" in action_msg:
            break

        if "main_pbar" in action_msg:
            pbars["main_pbar"].update(action_msg["main_pbar"])
            continue

        if "child_pbar" in action_msg:
            name = action_msg["child_pbar"]
            action = action_msg["action"] 
            if action == "start":
                desc = action_msg["desc"]
                position = pos_maker.get_next_pos()
                positions[name] = position
                size = action_msg["size"]
                if position in pbars:
                    pbars[position].close()
                    del pbars[position]
                pbars[position] = MultiTargetSocketIOTQDM(total=size, unit="B", unit_scale=True, leave=False, position=position+1, delay=1, desc=desc, source=source,socket_events=socket_events)
                continue
            if action == "update":     
                position = positions.get(name, None)
                if position == None:
                    debug_print(f"Do not have pbar for {name}")
                    for pname in positions:
                        debug_print(f"{pname} {positions[pname]}")
                    continue
                size = action_msg["size"]
                if position in pbars:
                    pbars[position].update(size)
                else:
                    debug_print(f"do not have pbar for {position}")
                continue
            if action == "close":
                position = positions.get(name, None)
                if position == None:
                    continue

                if position in pbars:
                    pbars[position].close()
                    del pbars[position]
                pos_maker.release_pos(position)

                del positions[name]
                continue
            continue 

    # final cleanup. Removes all managed pbars. 
    positions = pbars.keys()
    for position in positions:
        pbars[position].close()



def is_interface_up(interface:str) -> bool:
    """Determine if an interface is currently up

    Args:
        interface (str): name of a interface. 

    Returns:
        bool: True if interface is up, False if not
    """
    path = f"/sys/class/net/{interface}/operstate"
    try:
        with open(path, "r") as fid:
            state = fid.read()
    except NotADirectoryError:
        state = "down"

    state = state.strip()
    return state == "up"


def get_source_by_mac_address(robot_name:str) -> str:
    """Checks if a given interface is up or down

    Args:
        interface (str): Name of an interface from psutils.net_if_address()

    Returns:
        bool: True if interface is currently up, false if not
    """
    macs = []
    addresses = psutil.net_if_addrs()
    for interface in sorted(addresses):
        if interface == "lo":
            continue
        if not is_interface_up(interface):
            continue

        for addr in sorted(addresses[interface]):
            if addr.family == psutil.AF_LINK:  # Check if it's a MAC address
                macs.append(addr.address.replace(":",""))

    name = hashlib.sha256("_".join(macs).encode()).hexdigest()[:8]
    rtn = f"DEV-{robot_name}-{name}"
    return rtn


def getDateFromFilename(full_filename:str):
    """
    Extracts a date and time from a given filename using various patterns.

    Args:
        full_filename (str): The full path or name of the file.

    Returns:
        str: A formatted date-time string in the form 'YYYY-MM-DD HH:MM:SS' if a matching pattern is found, 
             or None if no date can be extracted.
    """

    # YYYY-MM-DD_HH.MM.SS
    pattern1 = r"^(\d{4})-(\d{2})-(\d{2})_(\d{2})\.(\d{2})\.(\d{2})\..*$"

    match = re.match(pattern1, os.path.basename(full_filename))
    if match:
        year, month, day, hh, mm, ss = match.groups()
        return f"{year}-{month}-{day} {hh}:{mm}:{ss}"

    # *YYYYMMDD_HHMMSS.###
    pattern3 = r".*(\d{8})_(\d{6})\..{3}$"
    match = re.match(pattern3, os.path.basename(full_filename))
    if match:
        year = match.group(1)[:4]
        month = match.group(1)[4:6]
        day = match.group(1)[6:8]

        hh = match.group(2)[:2]
        mm = match.group(2)[2:4]
        ss = match.group(2)[4:]
        return f"{year}-{month}-{day} {hh}:{mm}:{ss}"

    # *YYYYMMDD*
    pattern2 = r".*(\d{8})_*$"
    match = re.match(pattern2, full_filename)
    if match:
        yy = match.group(1)[:4]
        mm = match.group(1)[4:6]
        dd = match.group(1)[6:8]

        return f"{yy}-{mm}-{dd} 00:00:00"


    # *YYYY_MM_DD-HH_MM_SS*
    pattern4 = r"^.*(\d{4})_(\d{2})_(\d{2})-(\d{2})_(\d{2})_(\d{2}).*$"
    match = re.match(pattern4, os.path.basename(full_filename))
    if match:
        year, month, day, hh, mm, ss = match.groups()
        return f"{year}-{month}-{day} {hh}:{mm}:{ss}"

    match = re.match(pattern4, os.path.basename(os.path.dirname(full_filename)))
    if match:
        year, month, day, hh, mm, ss = match.groups()
        return f"{year}-{month}-{day} {hh}:{mm}:{ss}"

    return None


def _getMetaDataMCAP(filename: str, local_tz:str) -> dict:
    """
    Extracts metadata from an MCAP file, including message count by topic, 
    start and end timestamps in the specified local timezone.

    Args:
        filename (str): Path to the MCAP file.
        local_tz (str): Timezone to which the timestamps should be converted.

    Returns:
        dict: A dictionary with 'start_time' and 'end_time' in the local timezone, 
        and 'topics' with message counts per topic. Returns `None` if file reading fails.
    """
    with open(filename, "rb") as f:

        try:
            reader = make_reader(f)
        except mcap.exceptions.EndOfFile:
            return None 

        try:
            summary = reader.get_summary()
        except Exception as e:
            debug_print(f"Failed to read {filename} because {e}")
            return None

        if summary.statistics.message_end_time == 0:
            return None

        # mcap does not have a for free message count.  
        topics = {}
        for _, channel, message in reader.iter_messages():
            topic = channel.topic
            topics[topic] = topics.get(topic, 0) +1


        start_time_ros = summary.statistics.message_start_time
        end_time_ros = summary.statistics.message_end_time
        rtn = {
            "start_time": datetime.fromtimestamp(start_time_ros // 1e9, tz=timezone.utc).astimezone(pytz.timezone(local_tz)).strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": datetime.fromtimestamp(end_time_ros // 1e9, tz=timezone.utc).astimezone(pytz.timezone(local_tz)).strftime("%Y-%m-%d %H:%M:%S"),
            "topics": topics
        }
    return rtn


def _getMetadataROS(filename: str, local_tz:str) -> dict:
    """
    Extracts metadata from an ROS1 bag, including message count by topic, 
    start and end timestamps in the specified local timezone.

    Args:
        filename (str): Path to the ROS1 bag.
        local_tz (str): Timezone to which the timestamps should be converted.

    Returns:
        dict: A dictionary with 'start_time' and 'end_time' in the local timezone, 
        and 'topics' with message counts per topic. Returns `None` if file reading fails.
    """  
    try:  
        reader = AnyReader([pathlib.Path(filename)])
        reader.open()
    except AnyReaderError:
        return None 

    start_time_ros = reader.start_time
    end_time_ros = reader.end_time 
    topics = {topic: reader.topics[topic].msgcount for topic in sorted(reader.topics)}

    rtn = {
        "start_time": datetime.fromtimestamp(start_time_ros // 1e9, tz=timezone.utc).astimezone(pytz.timezone(local_tz)).strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": datetime.fromtimestamp(end_time_ros // 1e9, tz=timezone.utc).astimezone(pytz.timezone(local_tz)).strftime("%Y-%m-%d %H:%M:%S"),
        "topics": topics
    }

    return rtn 

def _getMetaDataJPEG(filename: str) -> dict:
    """Reads the timestamp from a JPEG file

    * First tries to extract from filename
    * Second tries to extract from JPEG metadata

    Args:
        filename (str): Path to JPEG file

    Returns:
        dict: A dictionary with 'start_time' and 'end_time' the file's time zone 
    """
    formatted_date =  getDateFromFilename(filename)
    if formatted_date:
        return {
            "start_time": formatted_date,
            "end_time": formatted_date
            }

    with open(filename, "rb") as f:
        tags = exifread.process_file(f)
    
    rtn = {}
    for tag in tags:
        if tag == "EXIF DateTimeOriginal":
            timestamp_str = str(tags[tag])
            rtn["start_time"] = timestamp_str
            rtn["end_time"] = timestamp_str
    return rtn

def _getMetaDataMP4(filename: str) -> dict:
    """Extracts the metadata for an MP4 video file

    Args:
        filename (str): Name of MP4 video file

    Returns:
        dict: A dictionary with 'start_time' and 'end_time' in the file's created timezone, 
    """
    try:
        probe = ffmpeg.probe(filename)
        if "streams" not in probe:
            return {}
        if "tags" not in probe["streams"][0]:
            return {}
        if "creation_time" not in  probe["streams"][0]["tags"]:
            return  {}

        formatted_date =  getDateFromFilename(filename)
        if formatted_date:
            creation_time = formatted_date
        else:
            creation_time = probe["streams"][0]["tags"]["creation_time"]
        duration = float(probe['format']['duration'])
        
        # Convert creation time to datetime object
        creation_datetime = datetime.fromisoformat(creation_time.replace('Z', '+00:00'))
        
        # Calculate end time by adding duration
        end_datetime = creation_datetime + timedelta(seconds=duration)
  
        rtn = {
            "start_time": creation_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_datetime.strftime("%Y-%m-%d %H:%M:%S")
        }
        return rtn 

    except ffmpeg.Error as e:
        # invalid file!
        return None

def _getMetaDataPNG(filename:str) -> dict:
    """Reads the timestamp from a PNG file

    * First tries to extract from filename
    * Second uses the Last Modified time from the filesystem.  

    Args:
        filename (str): Path to PNG file

    Returns:
        dict: A dictionary with 'start_time' and 'end_time' the file's time zone 
    """
    formatted_date =  getDateFromFilename(filename)
    if formatted_date is None:

        creation_date = datetime.fromtimestamp(os.path.getmtime(filename))
        formatted_date = creation_date.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "start_time": formatted_date,
        "end_time": formatted_date
        }


def _getMetaDataText(filename: str) -> dict:
    """Reads the timestamp from a TEXT file

    * First tries to extract from filename
    * Second uses the Last Modified time from the filesystem.  

    Args:
        filename (str): Path to TEXT file

    Returns:
        dict: A dictionary with 'start_time' and 'end_time' the file's time zone 
    """
    # have to use the file time for this one
    formatted_date =  getDateFromFilename(filename)
    if formatted_date is None:

        creation_date = datetime.fromtimestamp(os.path.getmtime(filename))
        formatted_date = creation_date.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "start_time": formatted_date,
        "end_time": formatted_date
        }


def _getMetaDataMarkdown(filename:str) -> dict:
    """Extract metadata from a Markdown file
    
    Since Markdown files don't have embedded timestamps, we use file modification time.
    
    Args:
        filename (str): Path to Markdown file
    
    Returns:
        dict: A dictionary with 'start_time' and 'end_time' based on file modification time
    """
    formatted_date = getDateFromFilename(filename)
    if formatted_date is None:
        creation_date = datetime.fromtimestamp(os.path.getmtime(filename))
        formatted_date = creation_date.strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        "start_time": formatted_date,
        "end_time": formatted_date
    }


def _getMetaDataCSV(filename:str) -> dict:
    """Extract metadata from a CSV file
    
    Since CSV files don't have embedded timestamps, we use file modification time.
    
    Args:
        filename (str): Path to CSV file
    
    Returns:
        dict: A dictionary with 'start_time' and 'end_time' based on file modification time
    """
    formatted_date = getDateFromFilename(filename)
    if formatted_date is None:
        creation_date = datetime.fromtimestamp(os.path.getmtime(filename))
        formatted_date = creation_date.strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        "start_time": formatted_date,
        "end_time": formatted_date
    }


def _getMetaDataGeneric(filename:str) -> dict:
    """Extract metadata from an unrecognized file extension
    
    Provides basic metadata using file modification time as a fallback for any file type
    that doesn't have a specialized metadata extractor.
    
    Args:
        filename (str): Path to file
    
    Returns:
        dict: A dictionary with 'start_time' and 'end_time' based on file modification time
    """
    formatted_date = getDateFromFilename(filename)
    if formatted_date is None:
        creation_date = datetime.fromtimestamp(os.path.getmtime(filename))
        formatted_date = creation_date.strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        "start_time": formatted_date,
        "end_time": formatted_date
    }


def getMetaData(filename:str, local_tz:str) -> dict:
    """Extract the metadata from a file

    Handles [mcap, bag, jpg, mp4, txt, ass, png, yaml, md, csv, and other file types]

    Args:
        filename (str): _description_
        local_tz (str): _description_

    Returns:
        dict: A dictionary with 'start_time' and 'end_time' in the local timezone, 
        and 'topics' with message counts per topic if applicable. For unrecognized file types,
        returns basic metadata with timestamps from file modification time. Returns `None` if file reading fails.
    """
    if filename.lower().endswith(".mcap"):
        return _getMetaDataMCAP(filename, local_tz)
    if filename.lower().endswith(".bag"):
        return _getMetadataROS(filename, local_tz)
    elif filename.lower().endswith(".jpg"):
        return _getMetaDataJPEG(filename)
    elif filename.lower().endswith(".mp4"):
        return _getMetaDataMP4(filename)
    elif filename.lower().endswith(".txt"):
        return _getMetaDataText(filename)
    elif filename.lower().endswith(".ass"):
        return _getMetaDataText(filename)
    elif filename.lower().endswith(".png"):
        return _getMetaDataPNG(filename)
    elif filename.lower().endswith(".yaml"):
        return _getMetaDataText(filename)
    elif filename.lower().endswith(".md"):
        return _getMetaDataMarkdown(filename)
    elif filename.lower().endswith(".csv"):
        return _getMetaDataCSV(filename)
    else:
        return _getMetaDataGeneric(filename)


def get_ip_address_and_port(server_address:str) -> Tuple[str, str]:
    """Get the IP address and port of a provided server:port string

    Args:
        server_address (str): A string in the form of "name:port"

    Returns:
        Tuple[str, str]: IP address, and port. 
    """
    ip_address = None
    port = None 
    if ":" in server_address:
        name, port = server_address.split(":")
    else:
        name = server_address

    try:
        ip_address = socket.gethostbyname(name)
    except socket.gaierror as e:
        pass 
    return ip_address, port

def same_adddress(server_address_1:str, server_address_2:str) -> bool:
    """Determines if two server address point to the same IP address

    Args:
        server_address_1 (str): name:port
        server_address_2 (str): name:port

    Returns:
        bool: True if both address resolve to same IP address and the ports are the same, False otherwise
    """
    ip_address_1, port_1 = get_ip_address_and_port(server_address_1)
    ip_address_2, port_2 = get_ip_address_and_port(server_address_2)

    if not ip_address_1 or not ip_address_2:
        return False 
    
    if not port_1 or not port_2:
        return False 
    
    if ip_address_1 != ip_address_2:
        return False 
    
    return port_1 == port_2

def address_in_list(query_address:str, server_list:List[str]) -> bool:
    """Determines if the query address is in the server list

    Args:
        query_address (str): A server address in the form of name:port
        server_list (List[str]): A list of server address in the form of name:port

    Returns:
        bool: True if query_address is in the server_list, False otherwise. 
    """
    ip_address_1, port_1 = get_ip_address_and_port(query_address)
    if not ip_address_1 or not port_1:
        return False 
    
    found = False
    for server_address_2 in server_list:
        ip_address_2, port_2 = get_ip_address_and_port(server_address_2)
        if not ip_address_2 or not port_2:
            continue 
        if ip_address_1 == ip_address_2 and port_1 == port_2:
            found = True
            break
    return found 
