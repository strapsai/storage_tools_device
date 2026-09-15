from typing import Tuple
from mcap.reader import make_reader
# from debug_print import debug_print
import os 
import subprocess
import platform

def get_mcap_binary() -> str:
    '''Find the name of the mcap binary for this platform'''

    # first look in /usr/local/bin/. This is for docker images
    filename = "/usr/local/bin/mcap"
    if os.path.exists(filename):
        return filename

    filename = None    
    arch = platform.machine()

    this_dir = os.path.dirname( __file__ )
    if arch == "x86_64":
        filename = os.path.join( this_dir, "..", "bin", "mcap-linux-amd64") 
    elif arch == "aarch64":
        filename = os.path.join( this_dir, "..", "bin", "mcap-linux-arm64")
    
    # debug_print(f"Arch: {arch}, filename: {filename}")
    if filename and os.path.exists(filename):
        return filename
    else:
        bin_dir = os.path.join( this_dir, "..", "bin")
        msg = f"Do not have mcap binary. Please download from https://github.com/foxglove/mcap/releases and put in {bin_dir}"
        raise FileNotFoundError(msg)


def test_mcap_file(filename:str) -> bool:
    """Wrapper function to test if an mcap file can be opened. 

    Args:
        filename (str): mcap filename

    Returns:
        bool: True if the file can be opened, False if not
    """
    with open(filename, "rb") as f:
        try:
            reader = make_reader(f)
            reader.get_summary()
        except Exception:
            return False
        
    return True


def recover_mcap(filename: str) -> Tuple[bool, str]:
    ''' Recover an mcap file

    Returns (status, message)
    '''
    
    # check to make sure we are using mcap files
    if not filename.endswith(".mcap"):
        msg = "filename does not end with '.mcap'. "
        return False, msg 

    # check to see if the original already exists. 
    orig = filename + ".orig"
    if os.path.exists(orig):
        msg = f"Original already exists. {orig}"
        return False, msg 

    # check to see if the mcap binary has been installed 
    mcap_bin = get_mcap_binary()
    if not mcap_bin:
        # debug_print("No binary")    
        return False, "No Binary"
    
    # remove the prior recovery, if it already exists. 
    recovery = filename + ".recovery"
    if os.path.exists(recovery):
        os.remove(recovery)

    # run the mcap recover action 
    cmd = [mcap_bin, "recover", filename, "-o", recovery]
    p = subprocess.run(cmd, capture_output=True)

    # verify that the file is readable. `mcap recover` may exit without writing
    # anything, so the recovery file is not guaranteed to exist.
    status = os.path.exists(recovery) and test_mcap_file(recovery)

    # all good, rename the original and put the recovery file in the right place. 
    if status:
        os.rename(filename, orig)
        os.rename(recovery, filename)
        return True, "ok"
    
    # if not all good, let the user know what happened. 
    if os.path.exists(recovery):
        os.remove(recovery)
    msg = f"file: {filename}, rc={p.returncode} " + p.stdout.decode("utf-8", errors="replace") + " " +  p.stderr.decode("utf-8", errors="replace")
    return False, msg 

