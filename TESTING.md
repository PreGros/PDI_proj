# Testy

Testy jsou spuštěny pomocí **bash** skriptu `test.sh`, který se nachází ve složce *spark*.

## Před spuštěním testu

1. **Instalace závislostí**:
   - Než začnete, nainstalujte všechny potřebné knihovny, závislosti a prerekvizity podle pokynů v souborech:
     - [README.md](./README.md)
     - [INSTALL.md](./INSTALL.md)

2. **Příprava prostředí**:
   - **Odstranění kontejnerů**: Před spuštěním testu je nutné vymazat všechny kontejnery pomocí příkazu:
     ```bash
     docker-compose down
     ```
   - **Poznámka**: Testový skript se postará o údržbu a vyčištění kontejnerů, takže mezi jednotlivými testy není potřeba tento příkaz znovu spouštět.

## Vstupní argumenty

Skript `test.sh` přijímá tři vstupní argumenty:

1. **První argument** (povinný): Určuje úkol (test) číslo, který se má otestovat. Hodnoty mohou být mezi 1 a 6.
2. **Dohodnuté argumenty** (volitelné): Dva další argumenty jsou potřebné pouze v případě specifické chyby, která je popsána na konci dokumentu.

## Průběh testování

- Skript automaticky spustí všechny kontejnery, které jsou potřebné pro daný test.
- Po dokončení testu skript kontejnery vypne a porovná výsledek s předpokládaným výstupem, který je uložen v souboru **testOutputs**.


## Výsledek

Test po porovnání vypíše, zda určený úkol prošel či ne. Při správném průchodu by měl vypsat zeleným písmem, že test prošel a červeným, že neprošel.

## Příklady použití

```bash
./test.sh 1
```

```bash
./test.sh 1 15 60
```

## Limity testu

Test nedostává informace od kontejneru, pouze předpokládá, že po defaultně nastaveném časovém okamžiku zpracuje testovací data. Pokud by test neprošel a v logu testu ve složce *testOutputs/assigment#/output#.txt* by byl posledním řádkem
```bash
pyspark-app  | 24/12/22 01:05:19 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, a408c3aec669, 44167, None)
```
tak spark-app nemělo dostatek času, aby stihlo zpracovat požadavek. Testovacímu skriptu lze potom dát druhý a třetí argument, aby počkal na dokončení požadavku o chvilku déle.
```bash
./test.sh 1 15 60
```
Zde skript počká 15 sekund před spuštěním **spark-app** a potom počká 60 sekund než ho terminuje. 

Defaultní nastavení testovacího skriptu je, že počká 15 sekund než spustí **spark-app** po **python-api-producer** a potom počká 45 sekund, než **spark-app** terminuje.

Zmíněný problém byl vážnější, když si **spark-app** ještě stahovala závislosti při každém vytváření. V nejaktuálnější verzi si je stahuje dopředu se spuštěním buildu.

## Důležité změny spark-app u testů

Při spouštění testů se **spark-app** výstup u úkolů 1,4 a 5 změní. První úkol má vypisovat data, která se právě objeví, ale kvůli charakteru testu se vypisuji všechny splňující danou podmínku od chodu aplikace. U úkolů 4 a 5, který pracuje s aktuálním časem, je použité pevné datum, aby se správně simuloval test. Mimo testování (při načítání dat z API) tyto úkoly pracují přesně podle zadání (nebo alespoň tak, jak je autor pochopil).