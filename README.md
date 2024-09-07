# sql-conn

Dataspliter, postgress model, analysis on the model all in python



## Getting Started

To get started with the project, you can either use Docker or set it up manually.

### Setting Up Environment Variables

1. **Copy `.env.example` to `.env`**:
    ```sh
    cp .env.example .env
    ```

2. **Edit the `.env` file** and add your PostgreSQL database URL:
    ```env
    DATABASE_URL=your_database_url
    ```

### Using Docker

1. **Clone the repository**:
    ```sh
    git clone https://github.com/youssefyr/sql-conn.git
    cd sql-conn
    ```

2. **Build the Docker image**:
    ```sh
    docker build -t sql-conn .
    ```

3. **Run the Docker container**:
    ```sh
    docker run -it --rm sql-conn
    ```

### Manual Setup

1. **Clone the repository**:
    ```sh
    git clone https://github.com/youssefyr/sql-conn.git
    cd sql-conn
    ```

2. **Install the required dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

3. **Install Jupyter Notebook** (if not already installed):
    ```sh
    pip install jupyter
    ```

4. **Run the scripts**:
    ```sh
    python src/run-all.py
    ```