# Zpracování proudu dat GTFS Realtime z dopravy PID pomocí Apache Spark

Tento projekt realizuje dotazování na data z dopravy **PID** (Přažská Integrovaná Doprava) a jejich následné zpracování jako proud dat. To je zajištěno se softwarem **Docker** a tří kontejnerů, kteří mezi sebou komunikují. Prvním je **python producer skript**, který se dotazuje API **PID** na informace o dopravě. Ten postupně posílá zvlášť informace o každém vozu. Posílá to druhému kontejneru, kterým je **Apache Kafka** (zde byla použita verze bitnami kafka). Ta zjednodušeně realizuje doručení ve správném pořadí informace o každém vozu třetímu kontejneru. Třetím a posledním kontejnerem je **spark-app** nebo-li apache spark-py skript, který informace příjme, zpracuje jako proud dat a vypíše daný výstup do konzole.

Projekt byl vytvořen v rámci předmětu **PDI** na **Fakultě informačních technologií Vysokého učení technického v Brně**.

Autorem je **Tomáš Zaviačič (xzavia00)**.

## Funkce
Při běžícím **Apache Kafka** kontejneru a **python producer skript** lze spustit **spark-app** kontejner, který podle čísla úkolu určeného v souboru *.env* vypíše vybraný výstup. (Více o spuštění v sekci [Jak postupovat při spouštění](#jak-postupovat-při-spouštění))


1. **Vozidla překračující rychlost 50km/h**:
   - průběžné vypisování vozidel, která překročila rychlost 50 km/h.
2. **Posledně hlášené zastávky tramvají**:
   - vypisuje seznam tramvají s ID jejich poslední hlášené zastávky a časem poslední aktualizace pro každou tramvaj hlášenou od startu aplikace.
3. **Nejrychlejších 5 vozů od startu aplikace**:
   - vypisuje seznam nejvýše 5 nejrychlejších vozů seřazených sestupně podle jejich posledně hlášené rychlosti od startu aplikace.
4. **Nejrychlejších 5 vozů za poslední 3 minuty**:
   - Vypisuje seznam nejvýše 5 nejrychlejších vozů hlášených během posledních 3 minut a seřazených setupně podle času jejich poslední aktualizace.
5. **Minimální a maximální zpoždění**:
   - vypisuje minimální a maximální zpoždění spočítané ze všech zpožděných vozů hlášených během posledních 3 minut.
6. **Nejdelší délka trasy u posledních 10 hlášených vozů**:
   - vypisuje nejdelší délku trasy, kterou z výchozích stanic ujelo posledních 10 hlášených vozů.

## Jak postupovat při spouštění
### Prerekvizity

Projekt byl vyvíjen a spouštěn na operačním systému Windows 11 s použitím Dockeru. Testy jsou prováděny pomocí bash skriptu, který byl spouštěn také na Windows 11 za pomocí WSL2. Projekt fungoval i při spouštění pomocí WSL2. 

- **Systémové požadavky na spuštění**: Linux/Windows11 operating system.
- **Systémové požadavky na testy**: Linux operating system.
- **Závislosti**:
  - Docker verze 24.0.6
  - GNU bash, verze 5.1.16
  - AWK, pokud není součástí GNU bash

### Instalace
Postupuj podle pokynů v souboru [INSTALL.md](./INSTALL.md).

### Spouštění aplikace

Je nutno podotknout před spuštěním, že čas zobrazený na výstupu bude vždy v UTC (koordinovaném světovém čase).

1. **Příprava prostředí:**
   - Ujistěte se, že máte vše nainstalováno v souladu s návodem v souboru [INSTALL.md](./INSTALL.md) a že všechny závislosti jsou správně nainstalovány.
   - Spouštění kontejnerů i testů se provádí ze složky spark (viz. [Hierarchie adresářů](#hierarchie-adresářů))

2. **Úprava souboru `.env`:**
   - Otevřete soubor `.env` a upravte proměnnou prostředí `ARG1_VAL` na číslo úkolu (1-6), který chcete spustit. Seznam úkolů naleznete v sekci [Funkce](#funkce).
   - Soubor `.env` obsahuje také proměnnou prostředí `MODE_VAL`, u které hodnota `api` zapne **python producer skript** a **spark-app** v módu pro čtení z API. Hodnota `local` se používá pro testování a při spouštění testů se přepíná sama.

3. **Sestavení a spuštění kontejnerů:**
   - Nejprve sestavte **python-api-producer** kontejner, který automaticky sestaví také **Apache Kafka** kontejner. Tento proces můžete spustit s volitelným přepínačem `-d`, aby kontejnery běžely na pozadí.
   
   - Příkaz pro spuštění:
     ```bash
     docker-compose up python-api-producer [-d]
     ```

4. **Spuštění spark-app:**
   - Po zhruba 10 sekundách se v **Apache Kafka** vytvoří *topic*, na který se bude připojovat **spark-app**.
   
   - Spusťte **spark-app** pomocí následujícího příkazu v novém terminálu (nebo ve stejném, pokud byl použit přepínač `-d`):
     ```bash
     docker-compose up spark-app
     ```

5. **Zpracování dat:**
   - **Spark-app** se automaticky připojí k **Apache Kafka** a po chvíli začne vypisovat požadovaný výstup z proudového zpracování dat.

6. **Ukoncení aplikace a znovu spuštění:**
   - Pro ukončení **spark-app** použijte klávesovou zkratku *CTRL+C*. Po ukončení můžete změnit úkol pro výpis úpravou souboru `.env` a znova spustit.

7. **Zastavení všech kontejnerů:**
   - Po dokončení práce s kontejnery je zastavíte pomocí příkazu:
     ```bash
     docker-compose down
     ```


## Testy
- Všechny informace a postup lze najít v [TESTING.md](./TESTING.md).

## Soubory v adresáři /spark

- .env
- docker-compose.yaml
- Dockerfile.producer
- Dockerfile.spark
- requirements.txt
- send_data.py *(python-api-producer)*
- test.sh
- wait-for-it.sh *(skript pro správné fungování producera, aby počkal na inicializaci kafku)*

## Soubory v adresáři /pyspark

- stream_pyspark *(spark-app)*

## Hierarchie adresářů

```bash
Spark
    ├───pyspark
    ├───testData
    └───testOutputs
        ├───assigment1
        ├───assigment2
        ├───assigment3
        ├───assigment4
        ├───assigment5
        └───assigment6
```

## Zdroje a licence
- [Apache Spark](https://spark.apache.org/) (Licence: Apache License 2.0)
- [Bitnami Kafka](https://bitnami.com/stack/kafka) (License: Apache License 2.0)
- [Wait-for-it](#https://github.com/vishnubob/wait-for-it)