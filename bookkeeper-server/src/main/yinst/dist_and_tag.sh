#!/bin/bash

PKG_FILE=$(find . -name '*.tgz' | head -1)
VERSION=$(find . -name '*.tgz' |grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
GIT_TAG="v$VERSION"

/home/y/bin/dist_install $PKG_FILE -batch -branch quarantine -group users -identity /home/y/conf/keydb/sherpa_build.keydb -headless
if [ $? -ne 0 ] ; then
	echo "Failed to push to dist"
	exit 1
fi

git tag $GIT_TAG
if [ $? -ne 0 ] ; then
        echo "Failed to create tag on git repo"
        exit 1
fi

git push origin $GIT_TAG
if [ $? -ne 0 ] ; then
        echo "Failed to push tag to git.corp"
        exit 1
fi

echo "Created git tag: $GIT_TAG"

