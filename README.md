# ATVES
This is a set of scripts that interface with Axsis and Conduent, and scrape values from those services into an internal database.

## Testing
To test, run `tox -- --axsis-user <AXSISUSERNAME> --axsis-pass <AXSISPASSWORD> --conduent-user <CONDUENTUSERNAME> --conduent-pass <CONDUENTPASSWORD> --report-user <FINANCIALUSER> --report-pass <FINANCIALPASS>`

If you are connected to the VPN, also add `--runvpntests` to run all tests
