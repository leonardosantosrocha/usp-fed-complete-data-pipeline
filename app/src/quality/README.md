# Data Quality — IRS Individual Income Tax (2014)

Validações aplicadas sobre o schema **raw** usando Great Expectations.

---

## Testes realizados

### 1. Schema: colunas esperadas
Verifica que todas as 120 colunas do dataset estão presentes. Se uma coluna estiver ausente, a validação falha antes de chegar nos demais testes.

### 2. Não nulos
Colunas `statefips`, `state`, `zipcode`, `agi_stub`, `year` não podem ser nulas.

### 3. Valores válidos
| Coluna | Regra |
|---|---|
| `statefips` | Código FIPS entre `01` e `56` |
| `state` | Uma das 51 siglas: AL, AK, AZ, AR, CA, CO, CT, DE, FL, GA, HI, ID, IL, IN, IA, KS, KY, LA, ME, MD, MA, MI, MN, MS, MO, MT, NE, NV, NH, NJ, NM, NY, NC, ND, OH, OK, OR, PA, RI, SC, SD, TN, TX, UT, VT, VA, WA, WV, WI, WY, DC |
| `agi_stub` | Apenas `1, 2, 3, 4, 5, 6` |
| `year` | Apenas `2014` |
| `zipcode` | 5 dígitos numéricos |

### 4. Contagens (`N*`) >= 0
Colunas `N*` contam quantas declarações reportaram determinado item (salário, dividendo, crédito etc.). Contagem de pessoas nunca pode ser negativa — 63 colunas validadas.

### 5. Valores monetários (`A*`) >= 0
51 colunas de totais em dólares validadas com `min_value=0`. Exceções permitidas pois representam resultado líquido, que pode ser negativo em caso de prejuízo: `a00900` (negócio), `a01000` (capital), `a26270` (sociedades/S-corp).

### 6. Volume do dataset
Entre **100.000** e **200.000** linhas.

---

**Total: 245 expectations** | Checkpoint: `irs_income_tax_checkpoint`
