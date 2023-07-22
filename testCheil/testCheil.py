﻿from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
#from fastapi_jwt_auth import AuthJWT
#from fastapi_jwt_auth.exceptions import AuthJWTException
import polars as pl
#Secret key to configure jwt
#used ssh-keygen -t rsa to generete de key. phrase testcheil
#SECRET_KEY = "nXCUALjYNh94pwgxRRGkm9HjQutWyCo1hD6shMZRAtQ"

app = FastAPI()

# Add CORS middleware to allow requests from any origin (*)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

#configure JWT
#@AuthJWT.load_config
#def get_config():
#    return {
#        "secret_key": SECRET_KEY,
#        "algorithm": "HS256"
#    }

#First
csv_path = "testCheil/files/vehicle.csv"
parquet_path = "testCheil/files/vehicles.parquet"
# Read file CSV in a DataFrame Polars
df = pl.read_csv(csv_path)
# Save DataFrame in format PARQUET
df.write_parquet(parquet_path)
print(df)

#Second
lazy_df = pl.read_parquet(parquet_path)
# Realizar la limpieza de datos eliminando registros incompletos
cleaned_df = lazy_df.drop_nulls()
# Mostrar el DataFrame resultante
print("Clened data")
print(cleaned_df)


#Third
# Lista de campos para calcular la desviación estándar
campos_desviacion_estandar = [
    "compactness", "circularity", "distance_circularity", "radius_ratio",
    "pr.axis_aspect_ratio", "max.length_aspect_ratio", "scatter_ratio",
    "elongatedness", "pr.axis_rectangularity", "max.length_rectangularity",
    "scaled_variance", "scaled_variance.1", "scaled_radius_of_gyration",
    "scaled_radius_of_gyration.1", "skewness_about", "skewness_about.1",
    "skewness_about.2", "hollows_ratio"
]
 # Ensure that the DataFrame contains the 'compactness' column
print("before show")
if "compactness" in cleaned_df:
        # Calcular la desviación estándar agrupada por el campo "class"
        print("have data")
        desviacion_estandar_df = (
            cleaned_df.groupby("class")
            .agg(
                **{campo: pl.col(campo).std() for campo in campos_desviacion_estandar}
            )
        )
        # Mostrar el DataFrame resultante con la desviación estándar
        print(desviacion_estandar_df)
        #return {"data": desviacion_estandar_df.to_list()}
else:
        print({"error": "Column 'compactness' not found in the DataFrame."})


@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.get("/home")
async def read_root():
    return {"data": cleaned_df.to_struct("data").to_list(),"desviacion_estandar_df": desviacion_estandar_df.to_struct("data").to_list()}
##funtion to hadle autenthication errors
#@app.exception_handler(AuthJWTException)
#async def authjwt_exception_handler(request, exc):
#    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})

##endpoint to login and generete the token
#@app.post("/login")
#async def login(Authorize: AuthJWT = Depends()):
#    # Aquí puedes implementar la lógica de autenticación de usuario.
#    # Si el usuario es válido, genera el token y lo retorna en la respuesta.

#    access_token = Authorize.create_access_token(subject="id_del_usuario")
#    return {"access_token": access_token}


##protected endpoint that requieres jwt token
#@app.get("/protected")
#async def protected(Authorize: AuthJWT = Depends()):
#    try:
#        Authorize.jwt_required()
#    except AuthJWTException as e:
#        raise HTTPException(status_code=e.status_code, detail=e.message)

#    # Si se pasa la validación, el usuario está autenticado y puede acceder a esta ruta protegida.
#    return {"message": "Ruta protegida, el usuario está autenticado."}

