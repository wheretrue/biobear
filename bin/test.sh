set -e

DOCKER_IMAGE="public.ecr.aws/localstack/localstack:latest"

# if venv exists source it, otherwise warn an continue
if [ -d "venv" ]; then
    echo "venv exists, sourcing..."
    source venv/bin/activate
else
    echo "venv does not exist, continuing..."
fi

# Setup the trap.
function teardown {
    echo "Tearing down..."

    # Find the docker container using the image.
    CONTAINER_ID=$(docker ps -q -f ancestor=$DOCKER_IMAGE)

    # Stop the container.
    docker stop $CONTAINER_ID

    echo "Teardown completed."
}

# check docker and aws cli are installed
if ! command -v docker &> /dev/null
then
    echo "docker could not be found"
    exit
fi

if ! command -v aws &> /dev/null
then
    echo "aws cli could not be found"
    exit
fi


# Setup
echo "Setting up..."

# Start the localstack container.
docker run -d -p 4566:4566 $DOCKER_IMAGE

# Wait for the stack to start.
sleep 2

trap teardown EXIT

# Create the test bucket.
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket

# Upload the test data.
aws --endpoint-url=http://localhost:4566 s3api put-object --bucket test-bucket --key test.fasta --body ./python/tests/data/test.fasta

# Make the bucket public.
aws --endpoint-url=http://localhost:4566 s3api put-bucket-acl --bucket test-bucket --acl public-read

# Create the parquet-bucket.
aws --endpoint-url=http://localhost:4566 s3 mb s3://parquet-bucket

# Make the bucket public.
aws --endpoint-url=http://localhost:4566 s3api put-bucket-acl --bucket parquet-bucket --acl public-read

pytest
