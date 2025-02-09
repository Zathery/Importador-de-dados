import pandas as pd
import psycopg2 as pg

from datetime import datetime
from unidecode import unidecode

# Leitura da planilha
planilha_def = pd.read_excel("dados_importacao.xlsx")
planilha_def.columns = planilha_def.columns.str.lower().str.strip()

# Conectando ao Banco
try:
    conectar = pg.connect(
        database="Desafio",
        user="postgres",
        password="sysadmin",
        host="localhost",  
        port="5432"        
    )
    cursor = conectar.cursor()
except Exception as e:
    print("Erro ao conectar ao banco:", e)
    exit()

registros = 0
falhas = 0
erros = []

# Deletar dados
delete_tbl_cliente_contatos =  """DELETE FROM public.tbl_cliente_contatos"""
cursor.execute(delete_tbl_cliente_contatos)
# delete_tbl_tipos_contato = """DELETE FROM public.tbl_tipos_contato"""
# cursor.execute(delete_tbl_tipos_contato)
delete_tbl_cliente_contratos = """DELETE FROM public.tbl_cliente_contratos"""
cursor.execute(delete_tbl_cliente_contratos)
# delete_tbl_planos = """DELETE FROM public.tbl_planos"""
# cursor.execute(delete_tbl_planos)
# delete_tbl_status_contrato = """DELETE FROM public.tbl_status_contrato"""
# cursor.execute(delete_tbl_status_contrato)
delete_tbl_clientes = """DELETE FROM public.tbl_clientes"""
cursor.execute(delete_tbl_clientes)
conectar.commit()


# Importando dados para as planilhas auxiliares
#comando_tbl_tipos_contato = """INSERT INTO tbl_tipos_contato ("tipo_contato") VALUES ('Telefone')"""
#cursor.execute(comando_tbl_tipos_contato)
#comando_tbl_tipos_contato = """INSERT INTO tbl_tipos_contato ("tipo_contato") VALUES ('Celular')"""
#cursor.execute(comando_tbl_tipos_contato)
#comando_tbl_tipos_contato = """INSERT INTO tbl_tipos_contato ("tipo_contato") VALUES ('Email')"""
#cursor.execute(comando_tbl_tipos_contato)
#comando_id_tipo_contato = """SELECT * FROM tbl_tipos_contato"""
#cursor.execute(comando_id_tipo_contato)
#id_resultado = cursor.fetchall()
#
#id_telefone = id_resultado[0][0]
#id_celular = id_resultado[1][0]
#id_email = id_resultado[2][0]
#plano_info = planilha_def[["plano", "plano valor"]].dropna().drop_duplicates().values.tolist()
#for item in plano_info:
#    plano = item[0]
#    valor = item[1]
#    comando_plano = """INSERT INTO tbl_planos ("descricao", "valor") VALUES (%s, %s)"""
#    cursor.execute(comando_plano,
#                   (plano, valor))
#
#status_info = planilha_def[["status"]].dropna().drop_duplicates().values.tolist()
#for item in status_info:
#    status_plano = item[0]
#    comando_status = """INSERT INTO tbl_status_contrato ("status") VALUES (%s)"""
#    cursor.execute(comando_status,
#                   (status_plano,))
#conectar.commit()  

# Função para tratar e validar o CPF/CNPJ
def validar_cpf_cnpj (cpf_cnpj):
    if isinstance (cpf_cnpj, int):
        cpf_cnpj = str(cpf_cnpj)
        if len(cpf_cnpj) <= 11:
            if len(cpf_cnpj) < 11 and len(cpf_cnpj) >= 7:
                cpf_cnpj = cpf_cnpj.zfill(11)
                cpf_cnpj = cpf_cnpj[:3] + '.' + cpf_cnpj[3:6] + '.' + cpf_cnpj[6:9] + '-' + cpf_cnpj[9:]
        
        elif len(cpf_cnpj) >= 12 or len(cpf_cnpj) <= 14:
            cpf_cnpj = cpf_cnpj.zfill(14)
            cpf_cnpj = cpf_cnpj[:2] + '.' + cpf_cnpj[2:5] + '.' + cpf_cnpj[5:8] + '/' + cpf_cnpj[8:12] + '-' + cpf_cnpj[12:]
        else:
            falhas = falhas + 1
            return "CPF/CNPJ inválido."
    else:
        if len(cpf_cnpj) <= 14:
            if len(cpf_cnpj) < 14 and len(cpf_cnpj) >= 12:
                cpf_cnpj = cpf_cnpj.zfill(14)
        
        elif len(cpf_cnpj) <= 18:
            if len(cpf_cnpj) < 18 or len(cpf_cnpj) >= 16:
                cpf_cnpj = cpf_cnpj.zfill(18)
            else:
                falhas = falhas + 1
                return "CPF/CNPJ inválido."
        else:
            falhas = falhas + 1
            return "CPF/CNPJ inválido."

        
    return cpf_cnpj

def obter_uf(estado: str):
    estado = unidecode(estado)
    estados_para_siglas = {
        "acre": "AC", "alagoas": "AL", "amapa": "AP", "amazonas": "AM",
        "bahia": "BA", "ceara": "CE", "distrito federal": "DF", "espirito santo": "ES",
        "goias": "GO", "maranhao": "MA", "mato grosso": "MT", "mato grosso do sul": "MS",
        "minas gerais": "MG", "para": "PA", "paraiba": "PB", "parana": "PR",
        "pernambuco": "PE", "piaui": "PI", "rio de janeiro": "RJ", "rio grande do norte": "RN",
        "rio grande do sul": "RS", "rondonia": "RO", "roraima": "RR", "santa catarina": "SC",
        "sao paulo": "SP", "sergipe": "SE", "tocantins": "TO"
    }
    return estados_para_siglas.get(estado, "UF não encontrada")

def obter_id(tabela: str, campo: str, valor: str):
        consulta_id = f"""SELECT ID FROM {tabela} WHERE {campo} = %s"""
        cursor.execute(consulta_id, (valor,))
        resultado_id = cursor.fetchone()
        id = str(resultado_id[0])
        
        return id

def importar_dados(vencimento: str, isento: str,
                   endereco: str, numero_end: str, bairro: str, cidade: str, complemento: str, cep: str, uf: str, telefone: str, celular: str, email: str, registros: str):
    id_cliente = obter_id('tbl_clientes', 'cpf_cnpj', cpf_cnpj)
    id_plano = obter_id("tbl_planos", "descricao", plano)            
    id_status = obter_id("tbl_status_contrato", "status", status)
    id_telefone = obter_id("tbl_tipos_contato", "tipo_contato", "Telefone")
    id_celular = obter_id("tbl_tipos_contato", "tipo_contato", "Celular")
    id_email = obter_id("tbl_tipos_contato", "tipo_contato", "Email")                 
    
    comando_tbl_cliente_contratos = """ INSERT INTO tbl_cliente_contratos ("cliente_id","plano_id","status_id","dia_vencimento", "isento", "endereco_logradouro", "endereco_numero", "endereco_bairro", "endereco_cidade", "endereco_complemento", "endereco_cep", "endereco_uf") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
    cursor.execute(comando_tbl_cliente_contratos, 
                    (id_cliente, id_plano, id_status, vencimento, isento, endereco, numero_end, bairro, cidade, complemento, cep, uf,))
    if telefones != '':
        importar_contatos(id_cliente, id_telefone,telefones)
    
    if celulares != '':
        importar_contatos(id_cliente, id_celular, celulares)
    
    if emails != '':
        importar_contatos(id_cliente, id_email,emails)
    
    registros = registros + 1

def importar_contatos(id_cliente: str, id_cont: str, contato: str):
        comando_tbl_clientes_contatos = """ INSERT INTO tbl_cliente_contatos ("cliente_id", "tipo_contato_id", "contato") VALUES (%s, %s, %s) """
        cursor.execute(comando_tbl_clientes_contatos,
    				   (id_cliente,id_cont,contato))

        


# Iterando sobre os registros da planilha
for i in range(len(planilha_def)):
    try:
        nome_razao_social = planilha_def.at[i, "nome/razão social"]
        fantasia = planilha_def.at[i, "nome fantasia"]
        cpf_cnpj = planilha_def.at[i, "cpf/cnpj"]
        data_nasc = planilha_def.at[i, "data nasc."]
        data_cadastro_cli = planilha_def.at[i, "data cadastro cliente"]
        celulares = str(planilha_def.at[i,'celulares']).replace("nan","")
        telefones = str(planilha_def.at[i,'telefones']).replace("nan","")
        emails = str(planilha_def.at[i,'emails']).replace("nan","")
        endereco = str(planilha_def.at[i,'endereço']).replace("nan","")
        numero_end = str(planilha_def.at[i,'número']).replace("nan","")
        complemento = str(planilha_def.at[i,'complemento']).replace("nan","")
        bairro = str(planilha_def.at[i,'bairro']).replace("nan","")
        cep = str(planilha_def.at[i,'cep']).replace("nan","")
        cidade = str(planilha_def.at[i,'cidade']).replace("nan","")
        uf = str(planilha_def.at[i,'uf']).lower().replace("nan","")
        plano = str(planilha_def.at[i,'plano']).replace("nan","")
        plano_valor = str(planilha_def.at[i,'plano valor']).replace("nan","")
        vencimento = str(planilha_def.at[i,'vencimento']).replace("nan","")
        isento = str(planilha_def.at[i,'isento']).replace("nan","")
        status = str(planilha_def.at[i,'status']).replace("nan","")
        
        # Tratando o CPF/CNPJ
        cpf_cnpj = validar_cpf_cnpj(cpf_cnpj)
        if cpf_cnpj == ("CPF/CNPJ inválido."):
            falhas = falhas + 1    
            continue
        
        # Tratando as datas         
        if pd.isna(data_nasc):
            data_nasc = None  # Ou defina um valor padrão
        else:
            data_nasc = data_nasc.strftime('%Y-%m-%d')  # Convertendo para formato 'YYYY-MM-DD'

        if pd.isna(data_cadastro_cli):
            data_cadastro_cli = None  # Ou defina um valor padrão
        else:
            data_cadastro_cli = data_cadastro_cli.strftime('%Y-%m-%d')
        
        # Tratando CEP
        cep = cep.zfill(9)
        cep = cep[:5] + "-" + cep[6:]
        
        # Tratando contatos
        if telefones.lower() == "nan" or telefones.lower() == "none":
            telefones = ""  
        elif telefones.endswith(".0"):  
            telefones = telefones[:-2]  
        if telefones != "":
            telefones = "(" + telefones[:2] + ")" + telefones[3:7] + "-" + telefones[8:]
        
        if celulares.lower() == "nan" or celulares.lower() == "none":
            celulares = ""  # Substitui NaN por string vazia
        elif celulares.endswith(".0"):  
            celulares = celulares[:-2]  # Remove ".0" do final
        if celulares != "":
            celulares = "(" + celulares[:2] + ")" + celulares[3:8] + "-" + celulares[9:]
        
        # Tratando boolean
        if isento == '' or isento == 'nao' or isento == 'não':
            isento = 'false'
        elif isento == 'sim' or isento:
            isento = 'true'
        else:
            isento = 'false'
            
        #Tratando UF
        uf = obter_uf(uf)
        if len(uf) > 2:
            erro = "UF inválida"
            erros.append(f"Erro ao inserir registro {i}: {erro}")
            falhas = falhas + 1
            raise ValueError()
        
        # Criando o comando SQL
        comando = """INSERT INTO tbl_clientes ("nome_razao_social", "nome_fantasia", "cpf_cnpj", "data_nascimento", "data_cadastro") VALUES (%s, %s, %s, %s, %s)"""

        # Verificando se já existe um cliente com o mesmo CPF no banco
        consulta_cpf = """SELECT COUNT(*) FROM tbl_clientes WHERE cpf_cnpj = %s"""
        cursor.execute(consulta_cpf, (cpf_cnpj,))
        resultado = cursor.fetchone()

        if resultado[0] > 0:
            id_cliente = obter_id('tbl_clientes', 'cpf_cnpj', cpf_cnpj)
            consulta_end_cli = """SELECT endereco_logradouro, endereco_numero, endereco_bairro, endereco_cidade, endereco_uf FROM tbl_cliente_contratos WHERE cliente_id = %s"""
            cursor.execute(consulta_end_cli, (id_cliente,))
            info_end = cursor.fetchall()
            
            for linha in info_end:
                endereco_logradouro = linha[0]
                endereco_numero = linha[1]
                endereco_bairro = linha[2]
                endereco_cidade = linha[3]
                endereco_uf = linha[4]
                if (endereco_logradouro == endereco and
					endereco_numero == numero_end and
					endereco_bairro == bairro and
					endereco_cidade == cidade and
					endereco_uf == uf):
                    erro = (f"Registro com CPF/CNPJ {cpf_cnpj} já existe. Duplicidade de registros")                    
                    erros.append(f"Erro ao inserir registro {i}: {erro}")
                    falhas = falhas + 1
                    raise ValueError()
                else:
                    importar_dados(vencimento, isento, endereco, numero_end, bairro, cidade, complemento, cep, uf, telefones, celulares, emails, registros)
                    erro = (f"Registro com CPF/CNPJ {cpf_cnpj} já existe.")                    
                    erros.append(f"Erro ao inserir registro {i}: {erro}")
        else:
            # Criando o comando SQL 
            comando_tbl_clientes = """INSERT INTO tbl_clientes ("nome_razao_social", "nome_fantasia", "cpf_cnpj", "data_nascimento", "data_cadastro") 
                         VALUES (%s, %s, %s, %s, %s)"""
            cursor.execute(comando, 
                           (nome_razao_social, fantasia, cpf_cnpj, data_nasc, data_cadastro_cli))
            
            importar_dados(vencimento, isento, endereco, numero_end, bairro, cidade, complemento, cep, uf, telefones, celulares, emails, registros)
            registros = registros + 1
            
    except Exception as e:
        erros.append(f"Erro ao inserir registro {i}: {e}")
        falhas = falhas + 1

# Commit e fechamento da conexão
conectar.commit()
cursor.close()
conectar.close()
print("Importação concluída com sucesso!")
print("Foram importados %s registros com sucesso, e um total de %s falha(s)" %(registros, falhas))
if erros:
    for erro in erros:
        print(erro)