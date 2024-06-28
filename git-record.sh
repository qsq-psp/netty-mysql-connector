#!/bin/bash
echo "git command history records, do not execute"
exit 1
git config --global user.name "qsq-psp"
git config --global user.email "qsq-psp@qq.com"
ssh-keygen -t ed25519 -C "qsq-psp@qq.com"
ssh -T git@gitee.com
git clone git@gitee.com:qsq-psp/netty-mysql-connector.git
cd netty-mysql-connector
git config --local user.name "qsq-psp"
git config --local user.email "qsq-psp@qq.com"
git remote rename origin gitee
ssh -T git@github.com
git remote add github git@github.com:qsq-psp/netty-mysql-connector.git
git push -u github master