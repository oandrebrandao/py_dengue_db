import sqlite3 as conector
from modelos import Municipio, Dengue, Populacao

try:
    conexao=conector.connect('dados_dengue_db.db')
    conexao.execute("PRAGMA foreign_keys = on")
    cursor=conexao.cursor()

    comando= '''CREATE TABLE  Municipio(
           codigo INTEGER NOT NULL,
           nome VARCHAR(32) NOT NULL,
           PRIMARY KEY (codigo)
           );'''
    cursor.execute(comando)
    
    comando='''CREATE TABLE Populacao(
            codigo INTEGER NOT NULL,
            ano INTEGER NOT NULL,
            populacao INTEGER NOT NULL,
            PRIMARY KEY (codigo, ano),
            FOREIGN KEY (codigo) REFERENCES Municipio (codigo)
            );'''
    cursor.execute(comando)
    
    comando= '''CREATE TABLE Dengue(
         codigo INTEGER NOT NULL,
         ano INTEGER NOT NULL,
         casos INTEGER NOT NULL,
         PRIMARY KEY (codigo, ano),
         FOREIGN KEY (codigo) REFERENCES Municipio(codigo)
         );'''
    cursor.execute(comando)
    
    with open("dados_dengue.csv") as file:
        file.readline()
        for line in file:
            codigo, nome, casos_2019, casos_2020 = line.strip().split(';')
            print(codigo, nome, casos_2019, casos_2020)
            municipio = Municipio(codigo, nome)
            comando= '''INSERT INTO Municipio VALUES (:codigo, :nome);'''
            cursor.execute(comando, vars(municipio))
    
            dengue_2019= Dengue(codigo, 2019, int(casos_2019)) 
            dengue_2020= Dengue(codigo, 2020, int(casos_2020))
            comando= '''INSERT INTO Dengue VALUES (:codigo, :ano, :casos);'''
            cursor.execute(comando, vars(dengue_2019))
            cursor.execute(comando, vars(dengue_2020))
    
    with open("populacao.csv") as file:
        file.readline()
        for line in file:
            codigo, nome, pop_2019, pop_2020 = line.strip().split(';')
            print= (codigo, nome, pop_2019, pop_2020)
            populacao_2019 = Populacao(codigo, 2019, int(pop_2019))
            populacao_2020 = Populacao(codigo, 2020, int(pop_2020))
            comando= '''INSERT INTO Populacao VALUES(:codigo, :ano, :populacao);'''
            cursor.execute(comando, vars(populacao_2019))
            cursor.execute(comando, vars(populacao_2020))
    conexao.commit()
            
except conector.OperationalError as erro:
        print("Erro Operacional", erro)
except conector.DatabaseError as erro:
        print("Erro de Banco de Dados", erro)

finally:
    if cursor:
        cursor.close()
    if conexao:
        conexao.close()