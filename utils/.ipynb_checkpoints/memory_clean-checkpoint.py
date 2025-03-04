def main():
    import gc
    import sys
    import os
    import psutil
    import time

    # Get the current process
    process = psutil.Process(os.getpid())
    
    # Print initial memory usage
    print(f"Initial memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

    # Kill other unnecessary Python processes
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if 'python' in proc.info['name'].lower() and proc.pid != os.getpid():
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    # Enable aggressive garbage collection
    gc.enable()
    gc.set_threshold(0, 0, 0)
    
    # Clear all variables in global and local namespace
    for name in list(globals()):
        if name not in ('main', '__name__', '__file__', '__builtins__'):
            del globals()[name]
    locals().clear()
    
    # Clear all modules except essential ones
    base_modules = {'os', 'sys', 'gc', 'psutil', '__main__', '__builtins__', 'time'}
    for mod in list(sys.modules.keys()):
        if mod not in base_modules:
            try:
                del sys.modules[mod]
            except:
                pass

    # Super aggressive garbage collection
    for _ in range(10):
        gc.collect(2)  # Full collection with all generations
        time.sleep(0.1)  # Give OS time to reclaim memory

    # Platform specific memory release
    if sys.platform.startswith('linux'):
        try:
            import ctypes
            libc = ctypes.CDLL('libc.so.6')
            # MALLOC_TRIM
            libc.malloc_trim(0)
            # Try to release memory to OS
            os.system('sync')  # Sync filesystem buffers
            with open('/proc/sys/vm/drop_caches', 'w') as f:
                f.write('3')  # Clear PageCache, dentries and inodes
        except:
            pass
    elif sys.platform == 'win32':
        try:
            import ctypes
            # More aggressive Windows memory cleanup
            ctypes.windll.kernel32.SetProcessWorkingSetSize(-1, -1)
            ctypes.windll.psapi.EmptyWorkingSet(-1)
        except:
            pass

    # Try to minimize memory usage with psutil
    try:
        # Set process to low priority to help OS reclaim memory
        process.nice(19)  # Lowest priority
        # Minimize working set
        process.memory_full_info()
        process.memory_info()
    except:
        pass

    # Final garbage collection
    gc.collect()
    
    # Wait a bit for OS to reclaim memory
    time.sleep(1)
    
    # Print final memory usage
    print(f"Final memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

    # Optionally, you can terminate the current process
    # Warning: this will kill the Python process
    # os.kill(os.getpid(), 9)  # Uncomment this line for most aggressive cleanup



if __name__ == '__main__':
    # On Linux, run with sudo for full effect:
    # sudo python memory_clean.py
    # On Windows, run as administrator:
    # python memory_clean.py
    main()