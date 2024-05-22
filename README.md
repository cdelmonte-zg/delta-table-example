# Delta Table + Change Data Feed (CDF) Example

## Getting Started

This guide will help you set up a test environment using Visual Studio Code (VSC) with a containerized environment. Follow these steps to build, start the container, and connect to it.

### Prerequisites

Make sure you have the following installed on your system:

- [Docker](https://www.docker.com/get-started)
- [Visual Studio Code](https://code.visualstudio.com/)
- [Docker Extension for Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-docker)
- [Remote - Containers Extension for Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- [Python Extension for Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=ms-python.python)
- [Jupyter Extension for Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter)

### Building the Docker Image

1. Open a terminal.
2. Navigate to the `docker` directory of this repository:
    ```sh
    cd ./docker
    ```
3. Build the Docker image by running the following command:
    ```sh
    docker compose build
    ```

### Starting the Container

1. In the terminal, ensure you are still in the `docker` directory.
2. Start the container by running:
    ```sh
    docker compose up
    ```

### Connecting to the Running Container with Visual Studio Code

1. Open Visual Studio Code.
2. Install the Docker, Remote - Containers, Python, and Jupyter extensions if you haven't already.
3. Open the Command Palette (`Ctrl+Shift+P` or `Cmd+Shift+P` on Mac).
4. Type and select `Remote-Containers: Attach to Running Container...`.
5. You will see a list of running containers. Select your container from the list.
6. Visual Studio Code will open a new window connected to your container. You can now work inside the container as if it were a local environment.

### Running the Jupyter Notebook

1. In Visual Studio Code, navigate to the `src` directory.
2. Open the notebook file `cdf-example.ipynb`.
3. You can now run the cells in the notebook interactively within the container environment.

### Stopping the Container

To stop the running container, you can either:

- In the terminal where you ran `docker compose up`, press `Ctrl + C`.
- Alternatively, you can stop the container using the Docker extension in Visual Studio Code:
  - Click on the Docker icon in the Activity Bar.
  - Under `Containers`, find your running container.
  - Right-click on the container and select `Stop`.

### Additional Information

- **Rebuilding the Image**: If you make changes to the Dockerfile or any other files that affect the image, you will need to rebuild the image by running `docker compose build` again.
- **Logs and Debugging**: You can view logs and debug the container using the Docker extension in Visual Studio Code or by running `docker compose logs` in the terminal.

For further information and troubleshooting, refer to the Docker and Visual Studio Code documentation.
