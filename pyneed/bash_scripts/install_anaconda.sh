# installs anaconda and creates a py37 environment with python=3.7
if ! command -v conda --version &> /dev/null
echo "Installing conda..."
then
    cd /tmp
    curl https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh --output anaconda.sh

    bash anaconda.sh -b -p ~/anaconda3
    rm -f anaconda.sh
fi


export SHELL_NAME=$(basename $SHELL)
echo "Preparing shell..."
eval "$(~/anaconda3/bin/conda shell.$SHELL_NAME hook)"
conda init
source ~/.$(SHELL_NAME)rc

echo "Updating conda..."
conda update -y -n base -c defaults conda
conda config --set auto_activate_base false

echo "Create python 3.7 environment as py37..."
conda create -yn py37 python=3.7

cd ~
echo "Done!"
