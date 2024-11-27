# Controle de Veiculos 

based on [virtual-b-tree repo](https://github.com/Felipe-gsilva/)

---
Implementação de Árvore B+ com Virtualização para Indexação de um Sistema de Locadora de Veículos

Esse codigo pode ser alterado da seguinte maneira:

_defines.h_ contem as informacoes gerais das structs utilizadas, juntamente com a maioria dos _defines do codigo
- ORDER pode ser alterada para mudar o tamanho das paginas, mas sera necessario recompilacao.
- DEBUG esta seta em 0, pois em 1 faz testes e print muita informacao de debug.
- MAX_ADDRESS tamanho maximo do endereco que um arquivo suporta (baseado no tamanho do linux)
- TAMANHO_$STAT$ representa o tamanho individual de cada campo de uma pagina

_queue.h_ contem P = tamanho da fila

## Para o usuario
Tem 3 funcoes disponiveis e "exportadas":
- b_insert (insere uma placa no sistema)
- b_search (busca por uma placa)
- b_remove (remove uma placa)

---
## Testando

Sera necessario ter o *CMAKE* instalado.
Rode estes comandos na maquina:
``` bash

cmake -S . -B target/
cmake --build target/
./target/VB-TREE

```
---

# Vehicle management (with Virtualized B-tree)

> Virtualized B+ TREE impl for vehicle car rent purposes

This code can be modified as follows:

_defines.h_ contains general information about the structs used, along with most of the code’s #define directives.

- ORDER can be changed to modify the page size, but recompilation will be necessary.
- DEBUG is set to 0; setting it to 1 enables tests and prints extensive debugging information.
- MAX_ADDRESS defines the maximum address size a file can support (based on Linux size limits).
- TAMANHO_$STAT$ represents the individual size of each field within a page.

- _queue.h_ contains P, the queue size.

## Testing

You will need *cmake* in order to run the code:

``` bash

cmake -S . -B target/
cmake --build target/
./target/VB-TREE

```
