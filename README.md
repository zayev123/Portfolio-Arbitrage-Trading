# Portfolio-Arbitrage-Trading

There are two branches:
- master
- dev

The 'master' branch is the primary branch that has the propper solution. However, it requires a local redis instance running on port 6379 in order to work.

If you do not have a local redis instance running on your computer, then switch over to the 'dev' branch.
This has an sqlite implementation which is python's in-house database. Therefore, it should be compatible with any computer.
However, the 'dev' branch is only suitable for testing purposes as sqlite is not an asynchronous database and therefore
not suitable for the problem at hand.

The process of running either branch is the same:

1. In the terminal of the project's root folder, run (assuming you have python3 installed):
      - pip install virtualenv
      - virtualenv xenv
      - source xenv/bin/activate
      - pip install -r requirements.txt

2. There are 2 main files in the root folder of the project:
      - main_socket_connector.ipynb
      - main_data_analyser.ipynb
   
   These are both jupiter notebooks with 2 cell blocks each.

3. first run the main_socket_connector.ipynb notebook (if asked for selecting kernel environment, select xenv)
      - run the first cell block to load the dependencies
      - run the second cell block to connect the websockets.
      - wait for a minute for the data to acumulate
      - without interupting or closing this file/cell-block move over to the next notebook 'main_data_analyser.ipynb'

4. In this next notebook 'main_data_analyser.ipynb' do the following:
      - run the first cell block to load the dependencies
      - run the second cell block to view a pandas dataframe
  
5. The Pandas dataframe displayed in the 'main_data_analyser.ipynb' notebook has the following properties:
      - It holds the latest data received by the websockets (assuming the 'main_socket_connector.ipynb' is running in the background)
      - The dataframe can simply be refreshed by rerunning this cellblock which then re-receives the latest values already populated in redis/sqlite
      - The dataframe is sorted for maximum potential arbitrage possibilities i.e:
            - the pairs that have the maximum possibility for arbitrage will be appearing on the top

6. In order to stop streaming, in the 'main_socket_connector.ipynb' notebook, hit the interrupt button:
      - The interrupt button is a square shaped button at the top of the notebook next to the restart button
      - if you are shown a warning "interrupting the kernel timed out", ignore it and hit cancel and patiently wait for the process to stop gracefully
