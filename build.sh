set -ex

rm -rf build/ */*.egg-info
#rm -rf dist

docker-compose build --pull --force-rm tox
docker-compose run --rm tox

python setup.py sdist bdist_wheel
