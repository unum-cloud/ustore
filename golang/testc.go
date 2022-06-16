package main

/*
#include <stdio.h>
#include <stdlib.h>

typedef struct {
    int a;
    int b;
} Foo;

void pass_array(Foo **in, int len) {
    for(int i = 0; i < len; i++) {
        printf("A: %d\tB: %d\n", (*in+i)->a, (*in+i)->b);
    }
}
*/
import "C"

import (
	"unsafe"
)

type Foo struct{ a, b int32 }

func main() {
	foos := []*Foo{{1, 2}, {3, 4}}
	C.pass_array((**C.Foo)(unsafe.Pointer(&foos[0])), C.int(len(foos)))
}
