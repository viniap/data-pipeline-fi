# Execution Steps

## Execution without Docker

### 1. Clone the repository
The first step is to clone this repository to a local folder. To do so, run the following command in the terminal (Linux) or PowerShell (Windows, as admin) inside the local folder:

```
git clone https://github.com/viniap/data-pipeline-fi.git
```

### 2. Create a Pyhton virtual environment
The second step is to create a virtual environment inside the repository folder. To do so, run the following commands in the terminal:

```
cd data-pipeline-fi

python3 -m venv .venv   # Linux
python -m venv .venv    # Windows
```

To activate the virtual environment, run the following command:

```
source .venv/bin/activate   # Linux
.\.venv\Scripts\Activate    # Windows
```

### 3. Install dependencies

To be able to run the application, you need to install all the dependencies. To do so, run the following command:

```
pip install -r src/requirements.txt
```

On Windows, if you get a permission related error during installation, run the above command again and it will finish installing the remaining packages.

### 4. Create credentials.yml

To be able to connect to a PostgreSQL database, you will need to create a `credentials.yml` file inside the `data-pipeline-fi/conf/local` folder. Inside the `credentials.yml` write:

``` yaml
# credentials.yml
postgresql_credentials:
  con: postgresql+psycopg2://user:password@host:port/database
```

Where the words `user`, `password`, `host`, `port` and `database` must be replaced by valid values in order to connect to a existing database.

### 5. Run

If you followed the previous steps, now you are able to run de project. Make sure the database already exists and is active. Write the following in the terminal or PowerShell:

```
kedro run
```

You can choose which months of 2022 you want to download the source files from https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/ by modifying the `data-pipeline-fi/conf/base/parameters/data_acquisition.yml` file.

## Execution with Docker (not working)

### 1. Clone the repository
The first step is to clone this repository to a local folder. To do so, run the following command in the terminal (Linux) or PowerShell (Windows, as admin) inside the local folder:

```
git clone https://github.com/viniap/data-pipeline-fi.git
```

### 2. Create credentials.yml

To be able to connect to a PostgreSQL database, you will need to create a `credentials.yml` file inside the `data-pipeline-fi/conf/local` folder. Inside the `credentials.yml` write:

``` yaml
# credentials.yml
postgresql_credentials:
  con: postgresql+psycopg2://postgres:password@host:5432/data_pipeline_fi
```

Where the words `password` and `host` must be replaced by valid values.

### 3. Edit the Dockerfile

Put the same password above in `line 10` of `Dockerfile`.

### 4. Build the image

Run the following command to build the docker image from Dockerfile.

```
kedro docker build
```

### 5. Run the container

```
kedro docker run
```
