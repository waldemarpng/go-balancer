module main

go 1.17

replace balancer => ../balancer

replace backend => ../backend

require balancer v0.0.0-00010101000000-000000000000

require backend v0.0.0-00010101000000-000000000000 // indirect
