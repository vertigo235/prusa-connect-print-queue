module github.com/vertigo235/prusa-connect-print-queue

go 1.26.11

require github.com/prusa3d/prusa-connect/sdk v0.0.0

// Local development against the adjacent Prusa Connect checkout.
replace github.com/prusa3d/prusa-connect/sdk => ../prusa-connect/sdk
