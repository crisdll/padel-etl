from .utils import convert_to_datetime, get_data, convert_to_dataframe, convert_to_base64,clean_string,extract_id_partido, convert_to_int
from datetime import date, timedelta

my_date = date.today()  #- timedelta(days=180)  # Fecha fija para los datos

def get_competition_id(name):
    competitions = convert_to_dataframe(get_data("GetListEntranscursoCompeticiones", {'v':'100'}))
    idcompeticion = competitions[competitions['name'].str.replace(" ", "").str.lower().str.contains(name)]['idcompeticion'].iloc[0]
    idcompeticion_base64 = convert_to_base64(idcompeticion)
    return idcompeticion, idcompeticion_base64

def get_categorias(id_base64):
    result = get_data("Get_Cats_Competi", {'v':id_base64})
    categories = convert_to_dataframe(result)[["idcategoria", "name", "genero"]].rename(columns={
        "idcategoria": "categoria_api_id",
        "name": "nombre",
        "genero": "genero"  
    })
    categories['fecha'] = my_date
    return categories
    

def get_clubs(id, categoria_api_id):
    result = get_data("LoadParejasCompeticiones", {'v':id,'ace':'undefined','cat':categoria_api_id,'type':'1'})
    clubs_df = convert_to_dataframe(result)[['nom']].rename(columns={'nom': 'nombre'})
    clubs_df['nombre'] = clubs_df['nombre'].apply(clean_string)
    clubs_df['categoria_api_id'] = categoria_api_id
    clubs_df['fecha'] = my_date
    return clubs_df

def get_enfrentamientos(id_base64, categoria_api_id):
    # Get matchups for a category
    result = get_data("GetResultadosEncuentros", {'v':id_base64,'cat':categoria_api_id,'ty':'7','tab':'2','ace':'0','jornada':'0','lugar':'0'})
    enfrentamientos_df = convert_to_dataframe(result)[["Fecha", "resul", "eq1", "eq2", "num_jornada", "verpartidos"]]
    
    # 1. First STEP: Extract and filter valid IDs
    enfrentamientos_df['temp_id'] = enfrentamientos_df['verpartidos'].apply(extract_id_partido)
    enfrentamientos_df = enfrentamientos_df[
        (enfrentamientos_df['temp_id'].notnull()) &           # No NaN/None
        (enfrentamientos_df['temp_id'] != '') &               # No string vacío
        (enfrentamientos_df['temp_id'] != '0') &              # No '0' string
        (enfrentamientos_df['temp_id'].astype(str) != 'nan')  # No 'nan' string
    ]

     # 2. Reneme columns
    enfrentamientos_df = enfrentamientos_df.rename(columns={
        'eq1': 'club_local_id', 
        'eq2': 'club_visitante_id', 
        'resul': 'resultado', 
        'Fecha': 'fecha_partido', 
        'num_jornada': 'jornada', 
        'temp_id': 'enfrentamiento_api_id'
    })

    # 3. Add additional columns
    enfrentamientos_df['fecha'] = my_date
    enfrentamientos_df['categoria_api_id'] = categoria_api_id
    
    # 4. Apply corresponding data types
    # Strings
    for col in ["club_local_id", "club_visitante_id", "resultado"]:
        enfrentamientos_df[col] = enfrentamientos_df[col].apply(clean_string)
    # Integers
    for col in ["jornada", "enfrentamiento_api_id"]:
        enfrentamientos_df[col] = enfrentamientos_df[col].apply(convert_to_int)
    # Datetime
    enfrentamientos_df['fecha_partido'] = enfrentamientos_df['fecha_partido'].apply( 
        lambda x: convert_to_datetime(x, format='%d/%m/%Y %H:%M')
    )

    # Delete 'verpartidos' column if still exists
    if 'verpartidos' in enfrentamientos_df.columns:
        enfrentamientos_df = enfrentamientos_df.drop('verpartidos', axis=1)
    
    return enfrentamientos_df

def get_resultados(enfrentamiento_id): 
    #1. Get match results
    result = get_data("GetPartidosEnfrentamientos", {'ide':enfrentamiento_id})
    result_df = convert_to_dataframe(result)[[
        "idpartido", "win", "nom11", "nom12", "nom21", "nom22",
        "s11", "s12", "s21", "s22", "s31", "s32", "orden", "puntos"]]
    #2. Rename columns
    result_df = result_df.rename(columns={
        "idpartido": "partido_api_id",
        "win": "is_local_ganador",
        "nom11": "nombre1_local",
        "nom12": "nombre2_local",
        "nom21": "nombre1_visitante",
        "nom22": "nombre2_visitante",
        "s11": "set1_local",
        "s12": "set1_visitante",
        "s21": "set2_local",
        "s22": "set2_visitante",
        "s31": "set3_local",
        "s32": "set3_visitante",
        "orden": "pista",
        "puntos": "puntos"
    })
    #3. Add additional columns
    result_df['fecha'] = my_date
    result_df['enfrentamiento_api_id'] = enfrentamiento_id

    #4. Apply corresponding data types
    # Integers
    int_columns = ['partido_api_id','set1_local', 'set1_visitante', 'set2_local', 'set2_visitante', 'set3_local', 'set3_visitante', 'puntos', 'pista']
    for col in int_columns:
        result_df[col] = result_df[col].apply(convert_to_int)
    #Booleans
    result_df['is_local_ganador'] = result_df['is_local_ganador'].apply(lambda x: True if int(x) == 1 else False)
    
    return result_df