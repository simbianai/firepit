#!/bin/bash -e

if [ ! -z $1 ];  then
   BRANCH=$1
   echo "Building $BRANCH"
   git checkout $BRANCH
else
   echo "Building current branch"
fi

rm -rf venv
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

VERSION=$(python setup.py --version)

WHL_FILE=dist/firepit-${VERSION}-py2.py3-none-any.whl
rm -f ${WHL_FILE}

python setup.py bdist_wheel

if [ ! -f ${WHL_FILE} ]; then
   echo "ERROR: ${WHL_FILE} was not generated"
   exit -1
fi

echo "-----------------------------------------------"
echo "File ${WHL_FILE} created"
