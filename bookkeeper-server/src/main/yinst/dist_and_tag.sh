#!/bin/bash

PKG_FILE=$(find . -name '*.tgz' | head -1)
VERSION=$(find . -name '*.tgz' |grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
GIT_TAG="v$VERSION"
PRODNAME="bookkeeper"
ENV_PROP="env.properties"

/home/y/bin/dist_install $PKG_FILE -batch -branch quarantine -group users -identity /home/tortuga/.ssh/id_dsa -headless
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

echo "PRODNAME=$PRODNAME" > $ENV_PROP
echo "VERSION=$VERSION" >> $ENV_PROP

echo "Created git tag: $GIT_TAG"

