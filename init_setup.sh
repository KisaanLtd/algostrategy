#!/bin/bash
echo "[$(date)]: START"
# Create a Python virtual environment with Python 3.10
echo "[$(date)]: Creating Python virtual environment with Python 3.10"
python3.10 -m venv myenv
# Activate the virtual environment
echo "[$(date)]: Activating the virtual environment"
source myenv/bin/activate
# Install the required Python packages from requirements.txt
echo "[$(date)]: Installing the requirements"
pip install -r requirements.txt
echo "[$(date)]: END"

# #!/bin/bash
# echo "[$(date)]: START"
# # Create Conda environment with Python 3.10
# echo "[$(date)]: Creating Conda environment with Python 3.10" 
# conda create --prefix ./myenv python=3.10 -y
# # Activate the Conda environment
# echo "[$(date)]: Activating the Conda environment"
# source activate ./myenv || conda activate ./myenv
# # Install the required Python packages from requirements.txt
# echo "[$(date)]: Installing the requirements"
# pip install -r requirements.txt
# echo "[$(date)]: END"









