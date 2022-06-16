package test

import "fmt"

func vals() (int, int) {
	return 3, 7
}

func main() {
	fmt.Println("hello world")

	var b, c int = 1, 2
	fmt.Println(b, c)

	i := 1
	for i <= 3 {
		fmt.Println(i)
		i = i + 1
	}

	for j := 7; j <= 9; j++ {
		fmt.Println(j)
	}

	if num := 9; num < 0 {
		fmt.Println(num, "is negative")
	} else if num < 10 {
		fmt.Println(num, "has 1 digit")
	} else {
		fmt.Println(num, "has multiple digits")
	}

	fmt.Print(vals()[1])

}
