from data_processor import run_all_processing
from db_manager import DatabaseManager

def main():
    # 1. Processamento de Dados (Pandas)
    # Lê os arquivos brutos, limpa e salva na pasta 'processed_data'
    print("--- INICIANDO PROCESSAMENTO DE DADOS ---")
    run_all_processing()
    
    # 2. Carga no Banco de Dados (Postgres/PostGIS)
    print("\n--- INICIANDO OPERAÇÕES DE BANCO DE DADOS ---")
    db = DatabaseManager()
    
    try:
        # Cria as tabelas (DROP IF EXISTS + CREATE)
        db.create_tables()
        
        # Carrega os dados tabulares (CSVs processados)
        db.load_csv_data()
        
        # Carrega as geometrias (Shapefiles)
        db.load_shapefiles()
        
        print("\nProcesso concluído com sucesso!")
        
    except Exception as e:
        print(f"Ocorreu um erro crítico: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()