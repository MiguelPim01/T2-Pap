import pandas as pd
import sparql_dataframe

from functools import reduce

# Aluno: Miguel Vieira Machado Pim

# Neste trabalho foram utilizadas as quatro funcionalidades pedidas: lambda, filter, map e reduce

# lambda foi utilizado nas regras 1, 2 e 4, assim como no pré-processamento do dataframe
# filter foi utilizado na regra 2, assim como no pré-processamento do dataframe
# map foi utilizado na regra 1
# reduce foi utilizado na regra 4

# Foram feitas também consultas fechadas para duas regras: 2 e 4

# Ao final foi disponibilizado uma função rodar_regras() que roda todas as regras. Se quiser que não rode todas, comente as que você não quiser que rodem

# Definindo o endpoint e a query
endpoint = "http://dbpedia.org/sparql"
query = """
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dbp: <http://dbpedia.org/property/>
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

select distinct ?nome ?paisNascimento ?dataNascimentoFormatted ?clubeNome ?posicaoLabel ?altura ?numeroCamisa ?allGols ?liga where {
  
  # Pegando as instâncias de SoccerPlayer e o nome do jogador.
  { ?sub rdf:type dbo:SoccerPlayer . }
  union
  { ?sub a dbo:SoccerPlayer . }
  ?sub rdfs:label ?nome .
  FILTER (LANG(?nome) = 'en') .
  
  # Pegando o país de nascimento do jogador.
  ?sub dbo:birthPlace ?localNascimento .
  OPTIONAL { 
    ?localNascimento dbo:country ?paisNascimentoURI . 
  	?paisNascimentoURI rdfs:label ?paisNascimento .
   	FILTER (LANG(?paisNascimento) = 'en') .
  }
  
  # Pegando a data de nascimento do jogador.
  ?sub dbp:birthDate ?dataNascimento .
  
  # Pegando o clube em que o jogador está jogando.
  ?sub dbp:currentclub ?clubeAtual .
  ?clubeAtual rdfs:label ?clubeNome .
  FILTER (LANG(?clubeNome) = 'en') .
  
  # Pegando a posição em que o jogador joga.
  ?sub dbp:position ?posicao .
  ?posicao rdfs:label ?posicaoLabel .
  FILTER (LANG(?posicaoLabel) = 'en') .
  
  # Pegando a altura do jogador.
  ?sub dbo:height ?altura .
  FILTER (datatype(?altura) = xsd:double) .
  
  # Pegando o número da camisa do jogador.
  ?sub dbp:clubnumber ?numeroCamisa .
  FILTER (datatype(?numeroCamisa) = xsd:integer) .
  
  # Pegando a lista de gols do jogador, onde apenas o máximo dessa lista representa a quantidade real de gols dele.
  ?sub dbp:goals ?allGols .
  FILTER (datatype(?allGols) = xsd:integer) .
  
  # Pegando a liga em que o jogador joga.
  ?clubeAtual dbo:league ?ligaURI .
  ?ligaURI rdfs:label ?liga .
  FILTER (LANG(?liga) = 'en') .
  
  
  # Filtrando variáveis que devem ser URIs.
  FILTER (isURI(?clubeAtual)) .
  FILTER (isURI(?paisNascimentoURI)) .
  
  # Transformando as datas de nascimento do formato dateTime para date.
  BIND (
    IF (datatype(?dataNascimento) = xsd:dateTime, STRDT(STR(?dataNascimento), xsd:date), ?dataNascimento)
    AS ?dataNascimentoFormatted
  )
}"""

print("Fazendo leitura dos dados da dbpedia ...")
# Fazendo a busca pelos jogadores no dbpedia e armazenando em df
df = sparql_dataframe.get(endpoint, query)

jogadores_list = df.to_dict(orient='records')

jogadores_filtrados = list(filter(lambda j: j['allGols'] == max(
    jogador['allGols'] for jogador in jogadores_list if jogador['nome'] == j['nome']), jogadores_list))

# Novo df com os jogadores repetidos pelo AllGols retirados
new_df = pd.DataFrame(jogadores_filtrados)



# Regra 1
# Nesta regra iremos procurar por todos os nomes de jogadores e seus anos de nascimento. Depois faremos a verificação se o ano é o mesmo e se os jogadores não são os mesmos
# A função map foi utilizada para tranformar as datas de nascimento no ano de nascimento do jogador
def jogadores_contemporaneos(df):
    return [(j1, j2) for j1, ano1 in zip(df['nome'], list(map(lambda date: int(date[:4]) if '-' in date else None, df["dataNascimentoFormatted"])))
                     for j2, ano2 in zip(df['nome'], list(map(lambda date: int(date[:4]) if '-' in date else None, df["dataNascimentoFormatted"])))
                     if j1 < j2 and ano1 == ano2]

# Regra 2 (para essa regra foi definida uma consulta fechada)
# Nesta regra iremos procurar por todos os nomes de jogadorese seus respectivos clubes, se os clubes forem iguais nós retornamos eles
# Para a consulta fechada foi utilizada a funcionalidade filter para pegar todos os jogadores que jogam no mesmo clube que o jogador passado no argumento

## Consulta aberta
def jogadores_parceiros_consulta_aberta(df):
    return [(j1, j2) for j1, clube1 in zip(df['nome'], df['clubeNome'])
                     for j2, clube2 in zip(df['nome'], df['clubeNome'])
                     if j1 < j2 and clube1 == clube2]

## Consulta fechada
def jogadores_parceiros_consulta_fechada(df, nomeJogador):
    return list(filter(lambda j: j != nomeJogador and df[df['nome'] == nomeJogador]['clubeNome'].values[0] == df[df['nome'] == j]['clubeNome'].values[0], df['nome']))

# Regra 3
# Nesta regra iremos procurar por todos os nomes de jogadores, seus clubes, país de nascimento e posição que joga
# Se os clubes forem iguais, os países iguais e a posição diferente então retornamos este jogador
def jogadores_super_parceiros(df):
    return [(j1, j2, pais) for j1, clube1, pais, pos1 in zip(df['nome'], df['clubeNome'], df['paisNascimento'], df['posicaoLabel'])
                            for j2, clube2, pais2, pos2 in zip(df['nome'], df['clubeNome'], df['paisNascimento'], df['posicaoLabel'])
                            if j1 < j2 and clube1 == clube2 and pais == pais2 and pos1 != pos2]

# Regra 4 (para essa regra foi definida uma consulta fechada)
# Nesta regra iremos verificar os jogadores que jogam de atacantes, tem camisa 9 e mais de 50 gols. Fazemos assim, uma máscara no dataframe.
# Para a consulta fechada foi utilizado o reduce() onde reduzimos os dados até sobrar apenas True ou False, ou seja, se o jogador passado como argumento
# é centro avante goleador ou não.

## Consulta aberta
def centro_avante_goleador_consulta_aberta(df):
    return df[(df['posicaoLabel'] == "Forward (association football)") &
              (df['numeroCamisa'] == 9) &
              (df['allGols'] > 50)]

## Consulta fechada
def centro_avante_goleador_consulta_fechada(df, nomeJogador):
    return reduce(lambda acc, j: acc or (j['nome'] == nomeJogador and j['posicaoLabel'] == "Forward (association football)" 
                                         and j['numeroCamisa'] == 9 and j['allGols'] > 50), df.to_dict(orient='records'), False)

# Regra 5
# Nesta regra iremos procurar por todos os nomes de jogadores, seu país de nascimento e clube
# Se eles jogarem na mesma posição e no mesmo clube retornamos eles
def jogadores_concorrentes(df):
    return [(j1, j2, pos, clube) for j1, pos, clube in zip(df['nome'], df['posicaoLabel'], df['clubeNome'])
                                    for j2, pos2, clube2 in zip(df['nome'], df['posicaoLabel'], df['clubeNome'])
                                    if j1 < j2 and pos == pos2 and clube == clube2]

# Regra 6
# Nesta regra iremos procurar por todos os nomes de jogadores, clube e liga
# Depois iremos verificar se jogam na mesma liga se seus clubes são diferentes e se eles não são parceiros vide regra 2
def jogadores_rivais(df):
    parceiros = jogadores_parceiros_consulta_aberta(df)
    return [(j1, j2, liga) for j1, clube1, liga in zip(df['nome'], df['clubeNome'], df['liga'])
                              for j2, clube2, liga2 in zip(df['nome'], df['clubeNome'], df['liga'])
                              if j1 < j2 and liga == liga2 and clube1 != clube2
                              and (j1, j2) not in parceiros and (j2, j1) not in parceiros]

# Regra 7
# Nesta regra iremos verificar se a posição do jogador é de goleiro, se ele tem mais de 1.90 de altura e se tem pelo menos um gol
# Aplicamos a máscara ao dataframe
def goleiro_bom(df):
    return df[(df['posicaoLabel'] == "Goalkeeper (association football)") &
              (df['altura'] >= 1.90) &
              (df['allGols'] >= 1)]

# Regra 8
# Nesta regra iremos verificar se o jogador veste a camisa 10 e se tem 0 gols na carreira
# Aplicamos essa máscara ao dataframe
def jogadores_camisa_10_da_shoppe(df):
    return df[(df['numeroCamisa'] == 10) & (df['allGols'] == 0)]

def rodar_regras():
    print("Rodando regra 1 ...")
    print(jogadores_contemporaneos(new_df)) # 1
    
    print("\nRodando regra 2 ...")
    print(jogadores_parceiros_consulta_aberta(new_df)) # 2 - aberta
    print(jogadores_parceiros_consulta_fechada(new_df, "Lucas Piazon")) # 2 - fechada
    
    print("\nRodando regra 3 ...")
    print(jogadores_super_parceiros(new_df)) # 3
    
    print("\nRodando regra 4 ...")
    print(centro_avante_goleador_consulta_aberta(new_df)) # 4 - aberta
    print(centro_avante_goleador_consulta_fechada(new_df, "Jeison Medina")) # 4 - fechada
    
    print("\nRodando regra 5 ...")
    print(jogadores_concorrentes(new_df)) # 5
    
    print("\nRodando regra 6 ...")
    print(jogadores_rivais(new_df)) # 6
    
    print("\nRodando regra 7 ...")
    print(goleiro_bom(new_df)) # 7
    
    print("\nRodando regra 8 ...")
    print(jogadores_camisa_10_da_shoppe(new_df)) # 8

rodar_regras()