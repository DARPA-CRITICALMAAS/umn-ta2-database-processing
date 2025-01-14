#!/bin/bash 

# Exit anytime when anything fails
set -e

raw_data=${1?Raw data file missing}
attribute_map=${2?Attribute map file missing}
folder_name=$3
file_name=$4
github_branch="new_data_$(date '+%Y%m%d%H%m%s')"

# Either use user defined file_name or raw_data name
if [ $file_name ]; then
    :
else
    file_name=$(basename $raw_data)
    file_name="${file_name%.*}"
    echo "File name is not declared. Defaulting" $file_name 
fi

# Either use user defined folder_name or default: db_unknown
if [ $folder_name ]; then
    echo "Processed data will be saved as"
    echo "  "$folder_name"/"$file_name".json"
else
    folder_name='db_unknown'
    echo "Folder name is not declared. Defaulting to db_unknown"    
fi

echo "Processed data will be saved as"
echo "  "$folder_name"/"$file_name".json"
echo ""

# Checking existence of minmod repository
echo "Checking minmod data repository"
data_directory='../ta2-minmod-data'

if [ -d $data_directory ]; then
    echo "  "$data_directory "does exist."
    cd ..
else
    echo "Cloning minmod data repository"
    cd ..
    git clone git@github.com:DARPA-CRITICALMAAS/ta2-minmod-data.git
    
fi

# Create new GitHub Branch for pushing in new data
echo "Creating branch $github_branch in minmod data repository"
cd ta2-minmod-data
git checkout main
git pull
git checkout $github_branch || git checkout -b $github_branch

# Creating folder with folder name under minmod data repo
mkdir data/mineral-sites/umn/$folder_name || cd ./
echo ""

cd ../umn-ta2-database-processing

# Creating temporary data folder with attribute map and raw data
mkdir ../tmp_data || cd ./
cp $raw_data ../tmp_data/ || cp -r $raw_data ../tmp_data/
cp $attribute_map ../tmp_data/
raw_data_filename=$(basename $raw_data)
attribute_map_filename=$(basename $attribute_map)

# Create Docker container to run the program
echo "Creating Docker container"
docker build -t ta2-processing .
container_id=$(docker run -dit ta2-processing)
echo ""

# Move temporary data into docker container
echo "Copying raw data and attribute map into Docker container"
docker cp ../tmp_data $container_id":/umn-ta2-database-processing"

run_script=$(cat <<END
echo "Running ProcMine: Database Preprocessing"
poetry run python3 process_data_to_schema.py --path_data ./tmp_data/$raw_data_filename --path_map ./tmp_data/$attribute_map_filename --output_filename $file_name
exit
END
)

# Running ProcMine with input data and attribute map
docker exec -it $container_id /bin/bash -c "$run_script"
echo ""

# Copying file from docker container to local
echo "Moving processed data file from Docker container to local"
docker cp $container_id":/umn-ta2-database-processing/output/"$file_name".large.json" ../ta2-minmod-data/data/mineral-sites/umn/$folder_name
echo ""

# Move to data directory
echo "Uploading processed data to minmod data repository"
cd ../ta2-minmod-data
git add --all
git commit -m "Adding preprocessed data"
git push --set-upstream origin $github_branch

git checkout main
git branch -D $github_branch
echo ""

# Stopping Docker container
echo "Terminating Docker container"
cd ../umn-ta2-database-processing
docker stop $container_id
echo ""

# Removing temporary data folder
cd ..
rm -r tmp_data

echo "Please go to https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data to create a pull request for the same as links to be merged to main"
