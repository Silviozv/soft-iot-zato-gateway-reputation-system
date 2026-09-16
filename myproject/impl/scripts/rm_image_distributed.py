import subprocess

# ==========================================
# CONFIGURAÇÕES
# ==========================================
# Nome exato da imagem que você quer excluir. 
# Dica: Se quiser apagar TODAS as versões (tags) de uma vez, coloque 
# apenas o nome do repositório, ex: "silviozv/soft-iot-gateway-zato"
IMAGEM_ALVO = "silviozv/soft-iot-gateway-zato:1.0.0"

# Lista de máquinas alvo do cluster
MAQUINAS_SSH = [f"larsid{i:02d}" for i in range(1, 17)]

def limpar_imagens_remotas():
    """Conecta via SSH e força a remoção da imagem especificada em cada máquina."""
    print(f"[*] Iniciando varredura para remover '{IMAGEM_ALVO}' em {len(MAQUINAS_SSH)} máquinas...\n")
    
    for maquina in MAQUINAS_SSH:
        print(f"[*] Conectando em {maquina}...")
        try:
            # 1. Verifica se a imagem existe na máquina remota obtendo o ID dela
            cmd_check = f"ssh {maquina} 'docker images -q {IMAGEM_ALVO}'"
            resultado_check = subprocess.check_output(cmd_check, shell=True, stderr=subprocess.DEVNULL).decode('utf-8').strip()
            
            if not resultado_check:
                print(f"  [-] A imagem não está presente na máquina {maquina}. Pulando.")
                continue
            
            # 2. Se a imagem existir, executa o comando de exclusão forçada (-f)
            print(f"  [!] Imagem encontrada (IDs: {resultado_check.replace(chr(10), ' ')}). Iniciando exclusão...")
            
            # O comando remoto combina a busca do ID com a remoção.
            cmd_rm = f"ssh {maquina} 'docker rmi -f $(docker images -q {IMAGEM_ALVO})'"
            
            # Executa a remoção capturando o retorno para saber se deu certo
            processo = subprocess.run(cmd_rm, shell=True, capture_output=True, text=True)
            
            if processo.returncode == 0:
                print(f"  [+] Sucesso! Imagem '{IMAGEM_ALVO}' removida da {maquina}.")
            else:
                print(f"  [-] Falha ao remover a imagem da {maquina}.")
                print(f"      Detalhe do Docker: {processo.stderr.strip()}")
                
        except subprocess.CalledProcessError:
            print(f"  [!] Erro de conexão. Falha ao acessar a máquina {maquina} via SSH.")
        except Exception as e:
            print(f"  [!] Erro inesperado ao processar a máquina {maquina}: {e}")

if __name__ == "__main__":
    limpar_imagens_remotas()
    print("\n[🚀] Processo de limpeza finalizado.")