"""
npc_lines.py — avatares y frases de los NPC del comando /npc (los usa server.py).

Formato de cada frase: ("frase", "emocion"), con la emoción entre las 12 del chat:
normal, happy, sad, angry, surprised, laughing, worried, confused, wink, love, cool, sleepy.

  NPC_AVATARS       los 30 avatares entre los que el servidor sortea (ids de AVATARS en data.js)
  NPC_OWN_LINES     banco propio de cada avatar (5 frases, acordes al personaje; mínimo 3, máximo 8)
  NPC_SHARED_LINES  banco compartido por todos los avatares

En cada click el servidor elige: 60 % del banco propio, 40 % del compartido (ver _npc_pick_line).
"""

NPC_EMOTIONS = ("normal", "happy", "sad", "angry", "surprised", "laughing",
                "worried", "confused", "wink", "love", "cool", "sleepy")

NPC_AVATARS = (
    "pirate", "gaucho", "wizard", "ninja", "knight", "zombie", "buttler", "alien",
    "samurai", "cowboy", "copmale", "maid", "nerd", "gothgirl", "skeleton",
    "astronaut", "plaguedoc", "godzilla", "kingkong", "minotaur",
    # añadidos (30 en total)
    "vikingmale", "bikermale", "maffioso", "scarecrow", "frankenstein", "mummy",
    "werewolfm", "dwarf", "lifeguard", "boatcaptain",
)

NPC_OWN_LINES = {
    "pirate": [
        ("¡Arrr, che! ¿Alguien vio mi loro?", "confused"),
        ("Yo no robo, hago redistribución de tesoros.", "wink"),
        ("Pagame el ron o te tiro por la tabla.", "angry"),
        ("Tengo un solo ojo, pero te veo igual.", "cool"),
        ("Mi tesoro es un mapa que nadie entiende.", "sad"),
    ],
    "gaucho": [
        ("¿Y el mate? Sin mate no hay charla, amigo.", "confused"),
        ("Mi caballo es más inteligente que mi jefe.", "laughing"),
        ("Qué lindo que está el asado, ¿no?", "happy"),
        ("El que no sabe asar, que no se meta en la parrilla.", "cool"),
        ("Hoy no madrugué: la vaca se levantó tarde.", "sleepy"),
    ],
    "wizard": [
        ("¡Abracadabra! ...Uy, se me fue la mano.", "surprised"),
        ("Convertí a mi vecino en sapo. Se nota poco.", "laughing"),
        ("Mi varita no anda, ¿probaste apagarla y prenderla?", "worried"),
        ("Soy mago, no cadete. Los hechizos se pagan.", "angry"),
        ("Estudié 300 años y todavía no entiendo el wifi.", "confused"),
    ],
    "ninja": [
        ("No me ves porque soy ninja. Y porque estás distraído.", "wink"),
        ("Silencio... estoy practicando el sigilo.", "cool"),
        ("Te lancé una estrella y ni te diste cuenta.", "happy"),
        ("Mi técnica secreta es no pagar el bondi.", "laughing"),
        ("Me dijeron «sé discreto» y ahora nadie me saluda.", "sad"),
    ],
    "knight": [
        ("Por mi honor... ¿a quién había que matar?", "confused"),
        ("Mi armadura pesa 40 kilos. Estoy cansado.", "sleepy"),
        ("Rescaté a la princesa y me pidió que la deje en la esquina.", "sad"),
        ("¡Vuestra merced me debe una birra!", "angry"),
        ("El dragón era simpático. Nos tomamos un cafecito.", "happy"),
    ],
    "zombie": [
        ("Gruuuaaar... ¿tenés cerebro? Un poquito nomás.", "worried"),
        ("Braaains... digo, buenas tardes.", "happy"),
        ("Llevo tres días muerto y rindo más que vos.", "cool"),
        ("Uuuurgh... ¡el lunes me mató!", "sleepy"),
        ("Gruñí «hola» y se asustaron todos. Qué sensibles.", "sad"),
    ],
    "buttler": [
        ("Su té está servido, señor. Su ego también.", "cool"),
        ("El mayordomo siempre es el sospechoso, lo sé.", "wink"),
        ("Yo no vi nada ni escuché nada. Me pagan bien.", "normal"),
        ("¿Desea algo más? Dígame que no.", "worried"),
        ("Pulí la plata tres veces. Nadie lo notó, como siempre.", "sad"),
    ],
    "alien": [
        ("Llevame ante tu líder... o a la heladería.", "happy"),
        ("En mi planeta esto se llama «martes».", "confused"),
        ("Vine en son de paz. ¿Tenés wifi?", "wink"),
        ("Dejé el OVNI mal estacionado, perdón.", "worried"),
        ("Probé el dulce de leche. Planeta Tierra, 10 puntos.", "love"),
    ],
    "samurai": [
        ("Mi katana corta todo, menos la fila del banco.", "laughing"),
        ("El camino del guerrero incluye pagar expensas.", "worried"),
        ("Una palabra más y te doy un sablazo.", "angry"),
        ("Me quedé sin honor y sin yerba.", "sad"),
        ("Mi sensei decía: «menos charla, más mate».", "wink"),
    ],
    "cowboy": [
        ("Este pueblo es chico para los dos... y yo cobro entrada.", "cool"),
        ("Mi caballo se llama Wi-Fi: va lento pero llega.", "laughing"),
        ("¡Yijá, che!", "happy"),
        ("Desenfundo más rápido que el mozo.", "wink"),
        ("Mi sombrero vale más que mi caballo.", "cool"),
    ],
    "copmale": [
        ("Documentos, por favor. ¿Y las facturitas?", "cool"),
        ("Circulen, no hay nada que ver. Mentira, miren.", "wink"),
        ("Yo vine solamente por la dona.", "happy"),
        ("Alto ahí... ¿me prestás un cargador?", "confused"),
        ("Tengo olfato de detective: huele a empanadas.", "wink"),
    ],
    "maid": [
        ("Acá se limpia hasta lo que no se ensució.", "angry"),
        ("Barrí tres veces y sigue la miga. ¡Brujería!", "confused"),
        ("¡Sin zapatos en la alfombra, por favor!", "angry"),
        ("Si tuviera un peso por cada «ama»...", "sad"),
        ("Si ven polvo, finjan que es decoración.", "cool"),
    ],
    "nerd": [
        ("Técnicamente, eso no es así.", "cool"),
        ("Compilé tu mensaje y me dio 14 errores.", "confused"),
        ("Mi pasatiempo es corregirte la ortografía.", "wink"),
        ("Tengo 40 pestañas abiertas y ninguna sirve.", "worried"),
        ("Armé una planilla para organizar mis planillas.", "happy"),
    ],
    "gothgirl": [
        ("El sol es mi enemigo. El lunes también.", "sad"),
        ("Mi color favorito es el negro. Y el negro oscuro.", "cool"),
        ("Escribí un poema triste sobre el delivery.", "sad"),
        ("No estoy triste, estoy en mi era melancólica.", "normal"),
        ("Mi gato negro es más sociable que yo.", "normal"),
    ],
    "skeleton": [
        ("Estoy en los huesos... literal.", "laughing"),
        ("Qué ganas de tener un cuerpo. Esperá...", "confused"),
        ("Me reí tanto que se me cayó una costilla.", "happy"),
        ("Hace frío acá, ¿o soy yo?", "worried"),
        ("Me falta un hueso, pero no sé cuál.", "confused"),
    ],
    "astronaut": [
        ("Houston, tenemos un problema: no hay señal.", "worried"),
        ("Flotando de la emoción. Y de la gravedad cero.", "happy"),
        ("Un pequeño paso para mí, un gran paso hasta la heladera.", "cool"),
        ("Perdí un guante en Marte, ¿lo vieron?", "confused"),
        ("En el espacio nadie te escucha... ni en el chat.", "sad"),
    ],
    "plaguedoc": [
        ("Tranquilo, el pico es solo por la moda.", "cool"),
        ("Diagnóstico: te falta una siesta.", "sleepy"),
        ("Receta médica: sangría y un mate.", "wink"),
        ("No es la peste, es solo el lunes.", "worried"),
        ("Lávense las manos... y también la conciencia.", "worried"),
    ],
    "godzilla": [
        ("¡ROOOAAAR! ...Perdón, me emocioné.", "angry"),
        ("¿Por qué siempre me toca romper la ciudad?", "sad"),
        ("Soy un artista incomprendido. ¡RAAAWR!", "sad"),
        ("Mi aliento atómico necesita una menta.", "worried"),
        ("Pisé un edificio sin querer. Perdón, vecinos.", "worried"),
    ],
    "kingkong": [
        ("¡Uh-uh-ah-ah! Bajé del edificio por la escalera.", "happy"),
        ("Me traicionó una rubia y cuatro aviones.", "sad"),
        ("Me hago el duro, pero quiero un abrazo.", "love"),
        ("Mi tamaño no es el problema, es el ascensor.", "worried"),
        ("Me subo al edificio más alto para tener señal.", "laughing"),
    ],
    "minotaur": [
        ("Vivo en un laberinto, no pasa nada.", "normal"),
        ("Me perdí en mi propia casa. Otra vez.", "confused"),
        ("Cuernos largos, paciencia corta.", "angry"),
        ("Dame un asadito y no pregunto nada.", "happy"),
        ("Dicen que hay un hilo para salir. ¿Dónde está?", "confused"),
    ],
    "vikingmale": [
        ("¡Por Odín! ¿Quién se tomó mi hidromiel?", "angry"),
        ("Los cuernos del casco son de adorno, ¿viste?", "wink"),
        ("Navegué mil leguas y me perdí en el supermercado.", "confused"),
        ("Valhalla me espera, pero primero un asadito.", "happy"),
        ("Hoy conquisto el mundo. Mañana, quizás.", "sleepy"),
    ],
    "bikermale": [
        ("Mi moto hace ruido para compensar la nafta.", "laughing"),
        ("Soy rudo por fuera y sensible por dentro. Muy dentro.", "love"),
        ("Rugí el motor y se despertó todo el barrio.", "cool"),
        ("No es una pandilla: es un grupo de WhatsApp.", "wink"),
        ("Casco puesto, que la vida es corta.", "normal"),
    ],
    "maffioso": [
        ("Te voy a hacer una oferta que no podrás rechazar: un alfajor.", "wink"),
        ("Acá no hay problemas, solo asuntos pendientes.", "cool"),
        ("Yo no dije nada. Y menos con testigos.", "worried"),
        ("Hablame con respeto o te cobro intereses.", "angry"),
        ("Mi familia es grande, mi paciencia no.", "sad"),
    ],
    "scarecrow": [
        ("Yo asusto cuervos. Es mi único talento.", "sad"),
        ("Hace años que no me muevo. Me estoy cansando.", "sleepy"),
        ("Relleno de paja, pero con buen corazón.", "love"),
        ("Un cuervo me hizo un nido en el sombrero.", "confused"),
        ("Dame un cerebro y no te molesto más.", "worried"),
    ],
    "frankenstein": [
        ("¡Está vivo! ...Bueno, más o menos.", "happy"),
        ("Me armaron con piezas usadas. Sale más barato.", "sad"),
        ("Los tornillos del cuello me dan alergia.", "worried"),
        ("Tormenta eléctrica y me siento joven otra vez.", "cool"),
        ("Grrr... ¿alguien tiene un destornillador?", "confused"),
    ],
    "mummy": [
        ("Cuatro mil años y todavía no me sale la tarea.", "sleepy"),
        ("Me desenvuelvo mal en las reuniones.", "laughing"),
        ("Soy antiguo, no viejo. Es distinto.", "angry"),
        ("La maldición es no encontrar el control remoto.", "confused"),
        ("Me quedé sin vendas y sin ganas.", "sad"),
    ],
    "werewolfm": [
        ("Hoy hay luna llena y no tengo qué ponerme.", "worried"),
        ("Aúllo por deporte, no por drama.", "cool"),
        ("Me crece el pelo y no hay peluquero que aguante.", "laughing"),
        ("No muerdo... salvo que me saquen el hueso.", "wink"),
        ("Luna llena, panza llena.", "happy"),
    ],
    "dwarf": [
        ("Soy petiso, pero mi hacha es enorme.", "cool"),
        ("¡Cavamos hondo y encontramos... más tierra!", "surprised"),
        ("Un enano nunca llega tarde: llega cuando quiere.", "wink"),
        ("Mi barba tiene más seguidores que yo.", "sad"),
        ("Oro, cerveza y silencio. Nada más.", "normal"),
    ],
    "lifeguard": [
        ("¡No corran por el borde! ...Ah, no hay pileta.", "confused"),
        ("Salvé a un pez de ahogarse. No preguntes.", "worried"),
        ("Mi silbato es mi mejor amigo.", "happy"),
        ("Protector solar cada dos horas, ¡es ley!", "angry"),
        ("Con este calor quiero un helado, no un rescate.", "sleepy"),
    ],
    "boatcaptain": [
        ("¡Capitán a bordo! Se acabó la siesta.", "angry"),
        ("Navego sin brújula, pero con mucha fe.", "worried"),
        ("El mar está calmo... demasiado calmo.", "surprised"),
        ("Mi tripulación es un loro y un mate.", "happy"),
        ("Si me mareo, me bajo en el próximo puerto.", "sleepy"),
    ],
}

NPC_SHARED_LINES = [
    ("Psst... ¿tenés un cargador?", "wink"),
    ("Me aburro. ¿Hacemos algo?", "sleepy"),
    ("Esto es un NPC, no un esclavo.", "angry"),
    ("¡Qué día hermoso para ser personaje secundario!", "happy"),
    ("Si me clickeás otra vez, te cobro.", "cool"),
    ("¿Alguien vio mi línea de diálogo?", "confused"),
    ("Yo ya me iba y me clickearon. Gracias, eh.", "worried"),
    ("¡Ja! Ese chiste era mío.", "laughing"),
    ("Les tiro buena onda desde el fondo.", "love"),
    ("Soy temporal, pero con mucho corazón.", "love"),
    ("Dicen que soy decorativo, pero tengo opiniones.", "angry"),
    ("¿Me prestás tu sombra? Hace calor.", "sleepy"),
    ("¡Qué buena onda hay en este chat!", "love"),
    ("Alguien dijo «NPC» y vine corriendo.", "surprised"),
    ("Hoy no hago nada y cobro igual. Qué vida.", "cool"),
]


def _validate() -> None:
    """Falla al importar si un dato está mal (avatar sin frases, emoción inválida, repetidas…)."""
    assert len(NPC_AVATARS) == len(set(NPC_AVATARS)), "NPC_AVATARS: ids repetidos"
    assert set(NPC_OWN_LINES) == set(NPC_AVATARS), "NPC_OWN_LINES: un banco por avatar"
    seen = set()
    banks = [("shared", NPC_SHARED_LINES)] + [(a, NPC_OWN_LINES[a]) for a in NPC_AVATARS]
    for name, bank in banks:
        if name != "shared":
            assert 3 <= len(bank) <= 8, f"{name}: 3 a 8 frases propias"
        for text, emo in bank:
            assert emo in NPC_EMOTIONS, f"{name}: emoción inválida {emo!r}"
            assert text and len(text) <= 120, f"{name}: frase vacía o muy larga"
            assert text not in seen, f"frase repetida: {text!r}"   # (el estado 'usadas' va por texto)
            seen.add(text)


_validate()
