#!/bin/bash
if [ $# -ne 1 ]; then
    echo "git-auto: commit message required"
    exit 1
fi
git add .
if [ $? -ne 0 ]; then
    echo "git-auto: git add failed"
    exit 1
fi
git commit -m "$1"
if [ $? -ne 0 ]; then
    echo "git-auto: git commit failed"
    exit 1
fi
git push gitee master
if [ $? -ne 0 ]; then
    echo "git-auto: git git push gitee failed"
fi
git push github master
if [ $? -ne 0 ]; then
    echo "git-auto: git push github failed"
fi