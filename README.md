## Virtual Environment 

Go to the working directory and open up the command interface: 

```
python -m venv venv
```

To activate the virtual environment: 

On Powershell

```
.\venv\Scripts\Activate.ps1
```

On Git Bash

```
cd venv/Scripts 

. activate 
```

Finally install the required packages by typing

```
pip install -r requirements.txt
```

## Database

In Mysql, create a database called `nba_data`. 



## Notebook

Open up Git bash or a Linux kernel and type `jupyter notebook`. You should be able to access `main.ipynb` and run the code. It hangs after the `cursor.execute(SQLSTATEMENT)` command. 