import sys
import subprocess
import importlib

def check_python_packages():
    required_packages = [
        'pandas', 'streamlit', 'psycopg2', 'kafka', 
        'duckdb', 'requests', 'dotenv', 'numpy', 'faker', 'docker'
    ]
    
    print("Checking Python packages...")
    for package in required_packages:
        try:
            if package == 'psycopg2':
                importlib.import_module('psycopg2')
            elif package == 'kafka':
                importlib.import_module('kafka')
            elif package == 'dotenv':
                importlib.import_module('python_dotenv')
            else:
                importlib.import_module(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} - MISSING")
            return False
    return True

def check_docker():
    try:
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker: {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker not working")
            return False
    except FileNotFoundError:
        print("❌ Docker not installed")
        return False

def check_docker_compose():
    try:
        result = subprocess.run(['docker', 'compose', 'version'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker Compose: {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker Compose not working")
            return False
    except FileNotFoundError:
        print("❌ Docker Compose not found")
        return False

def main():
    print("=== Final Setup Verification ===\n")
    
    print(f"Python version: {sys.version}")
    print(f"Virtual environment: {'✅ Active' if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) else '❌ Not active'}")
    
    all_good = True
    all_good &= check_python_packages()
    all_good &= check_docker()
    all_good &= check_docker_compose()
    
    if all_good:
        print("\n🎉 SETUP COMPLETE! Ready to start the project!")
        print("\nNext steps:")
        print("1. Initialize Git repository: git init")
        print("2. Create docker-compose.yml file")
        print("3. Start building the pipeline!")
    else:
        print("\n❌ Some issues need to be fixed before proceeding")

if __name__ == "__main__":
    main()