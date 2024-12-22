# Instalace

## Instalace

1. **Spuštění buildu**
   - Všechny nutné knihovny a závislosti pro kontejnery jsou uložené v *Dockerfile.spark* a *Dockerfile.producer*. Ty se spolu s buildem automaticky vyhodnotí.
   ```bash
   docker-file build
   ```

2. **Stáhnutí Docker obrazu (image)**
   - Pro stáhnutí obrazu stačí spustit stáhnutí pomocí **Dockeru**.
   ```bash
   docker-file pull
   ```

3. **Spuštění aplikace**
   - Pro spuštění aplikace následujte postup, který naleznete v [README.md](./README.md).
   - Pro spuštění aplikace následujte postup, který naleznete v [TESTING.md](./TESTING.md).