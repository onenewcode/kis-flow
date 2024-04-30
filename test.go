package main

import "fmt"

func main() {
	s := []int{100, 4, 200, 1, 3, 2}
	fmt.Println(longestConsecutive(s))
}

func longestConsecutive(nums []int) int {
	max_num := 0
	m := make(map[int]int)
	for _, v := range nums {
		ma := m[v-1] + 1
		if ma > max_num {
			max_num = ma
		}
		m[v] = ma
	}
	return max_num
}
