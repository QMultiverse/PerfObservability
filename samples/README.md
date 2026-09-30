# Sample messages

Synthetic only. Regenerate with `python scripts/make_samples.py`; never
hand-edit, and never add anything derived from real traffic.

Test BICs (`TEST...`) are not issued to real institutions, the company
names are invented, and every IBAN is generated to pass the mod-97
checksum that `schwifty` enforces.

Send one into the Hub with:

```bash
ess send --type pacs.008 --file samples/pacs008_eur.xml \
         --app-hdr samples/pacs008_eur.apphdr.xml
ess send --type MT103 --file samples/mt103_gbp.fin
```

| Files | Message type | Amount | UETR |
| --- | --- | --- | --- |
| `pacs008_eur.xml + pacs008_eur.apphdr.xml` | pacs.008.001.08 | EUR 1234.56 | 8e3b7512-175d-4c22-b1d5-effedcda26fb |
| `pacs008_usd.xml + pacs008_usd.apphdr.xml` | pacs.008.001.08 | USD 98765.43 | 215dc562-24d0-45b2-b88f-dce3e638bca4 |
| `pacs008_sanctions_hit.xml + pacs008_sanctions_hit.apphdr.xml` | pacs.008.001.08 | EUR 500000.00 | 8a0cb201-725c-4338-b690-be1eb8d90a1b |
| `pacs009_gbp.xml + pacs009_gbp.apphdr.xml` | pacs.009.001.08 | GBP 2500000.00 | d84cf328-e92d-424d-9482-dde9c5cc618b |
| `pacs009cov_eur.xml + pacs009cov_eur.apphdr.xml` | pacs.009.001.08COV | EUR 750000.00 | a341f7d1-92ba-49d4-b165-7729e04a52fd |
| `mt103_gbp.fin` | MT103 | GBP 4321.00 | 1634651c-0d7d-416d-8db3-07065cc9a1c0 |
| `mt103_chf.fin` | MT103 | CHF 15000.00 | 1d04bc6f-9066-4e59-a411-2626c70a70ef |
| `mt202_usd.fin` | MT202 | USD 3000000.00 | ca4f3dcf-662c-421d-aafa-601b4e8cda9d |
| `mt202cov_eur.fin` | MT202COV | EUR 850000.00 | aaee1c55-1aba-4e7b-9ecc-8a27f11d78d6 |
