import docker
import sys

def test_docker_integration():
    try:
        # Test Docker Python client
        client = docker.from_env()
        
        # Get Docker version
        version = client.version()
        print(f"✅ Docker Engine version: {version['Version']}")
        
        # List containers
        containers = client.containers.list()
        print(f"✅ Docker Python client working. Found {len(containers)} running containers.")
        
        print("🎉 Docker and Python integration successful!")
        return True
        
    except Exception as e:
        print(f"❌ Docker integration failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing Docker + Python integration...")
    success = test_docker_integration()
    if success:
        print("\n✅ Ready to start the project!")
    else:
        print("\n❌ Fix Docker issues before proceeding")