set -ex

rm -rf build/ */*.egg-info
#rm -rf dist

docker build -t trade-api-python-tox -f Dockerfile-tox .
docker run --rm -w /docker -v "$(pwd):/docker" trade-api-python-tox tox

python setup.py sdist bdist_wheel
