import subprocess
import time

def run_jar_file(jar_path):
    start_time = time.time()
    
    try:
        subprocess.run(['java', '-jar', jar_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    return execution_time

if __name__ == "__main__":
    jar_file_path = 'Coordinator.jar'
    
    print(f"Launching JAR file: {jar_file_path}")
    
    execution_time = run_jar_file(jar_file_path)
    
    print(f"Execution time: {execution_time} seconds")
