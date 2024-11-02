package checksum

import "testing"

func BenchmarkGo(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	x := [8]uint32{1, 2, 3, 4, 5, 6, 7, 8}
	y := [8]uint32{17, 18, 19, 20, 21, 22, 23, 24}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
	}
	b.StopTimer()
}

func BenchmarkAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	x := [8]uint32{1, 2, 3, 4, 5, 6, 7, 8}
	y := [8]uint32{17, 18, 19, 20, 21, 22, 23, 24}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
	}
	b.StopTimer()
}
