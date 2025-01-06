#!/bin/bash 

commit_mssg="update $(date '+%Y%m%d::%H:%m')"

git checkout dev_branch
git add --all
git commit -m $commit_msg
git push