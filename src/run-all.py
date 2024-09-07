import time
import subprocess
import threading

# Colors
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
RESET = '\033[0m'


def run_script(script_name, script_path, color, optionalCmd=''):
    chars = ['|', '/', '-', '\\']
    
    def animate():
        while still_processing:
            for char in chars:
                if not still_processing:
                    break
                print(f'\r{color}{char} | Trying to run {script_name}...', end='', flush=True)
                time.sleep(0.15)

    still_processing = True
    animation_thread = threading.Thread(target=animate)
    animation_thread.start()
    
    try:
        start_time = time.time() 
        # Run the script and check if it was successful
        time.sleep(0.3)
        if optionalCmd == '':
            subprocess.run(['python', script_path], check=True)
        else: 
            subprocess.run(optionalCmd, check=True)
        end_time = time.time() 
        elapsed_time = end_time - start_time  # Calculate the elapsed time
        result_char = '✔'
        result_text = f'Successfully ran in {elapsed_time:.2f} seconds'
        fcolor = GREEN
    except subprocess.CalledProcessError:
        result_char = '✘'
        result_text = 'Failed to run'
        fcolor = RED
    finally:
        still_processing = False
        animation_thread.join()
    
    # Print the result and reset the color of the text
    print(f'\n{fcolor}{result_char} | {result_text} - {script_name}')
    print(RESET, end='', flush=True)


run_script('DataSpliter.ipynb', '', YELLOW, [ "jupyter", "execute", "src/Extract/DataSpliter.ipynb"])
run_script('model.py','src/Model/model.py', YELLOW)
run_script('analysis.py','src/Model/analysis.py', YELLOW)

