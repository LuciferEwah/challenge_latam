import requests

# Información a enviar con tus datos
data = {
    "name": "Axel Ariel Ramirez Mora",
    "mail": "aarm.axel@gmail.com",
    "github_url": "https://github.com/LuciferEwah/challenge_latam"
}

# URL del endpoint
url = "https://advana-challenge-check-api-cr-k4hdbggvoq-uc.a.run.app/data-engineer"

# Realizar la solicitud POST
response = requests.post(url, json=data)

# Comprobar la respuesta
if response.status_code == 200:
    print("✅ Solicitud enviada exitosamente:", response.json())
else:
    print(f"❌ Error al enviar la solicitud. Código de estado: {response.status_code}")
    print("Mensaje:", response.text)
