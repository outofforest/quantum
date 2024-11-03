package checksum

import (
	"crypto/rand"
	"fmt"
	"io"
	"math/bits"
	"testing"
	"unsafe"
)

var (
	x      = [16]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	y      = [16]uint32{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	matrix = [16][16]uint32{
		{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
		{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f},
		{0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f},
		{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f},
		{0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f},
		{0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f},
		{0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f},
		{0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f},
		{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f},
		{0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f},
		{0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf},
		{0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf},
		{0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf},
		{0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf},
		{0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb, 0xec, 0xed, 0xee, 0xef},
		{0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff},
	}
	chunks = [16][48]uint32{
		{
			0x000, 0x001, 0x002, 0x003, 0x004, 0x005, 0x006, 0x007, 0x008, 0x009, 0x00a, 0x00b, 0x00c, 0x00d, 0x00e, 0x00f,
			0x010, 0x011, 0x012, 0x013, 0x014, 0x015, 0x016, 0x017, 0x018, 0x019, 0x01a, 0x01b, 0x01c, 0x01d, 0x01e, 0x01f,
			0x020, 0x021, 0x022, 0x023, 0x024, 0x025, 0x026, 0x027, 0x028, 0x029, 0x02a, 0x02b, 0x02c, 0x02d, 0x02e, 0x02f,
		},
		{
			0x100, 0x101, 0x102, 0x103, 0x104, 0x105, 0x106, 0x107, 0x108, 0x109, 0x10a, 0x10b, 0x10c, 0x10d, 0x10e, 0x10f,
			0x110, 0x111, 0x112, 0x113, 0x114, 0x115, 0x116, 0x117, 0x118, 0x119, 0x11a, 0x11b, 0x11c, 0x11d, 0x11e, 0x11f,
			0x120, 0x121, 0x122, 0x123, 0x124, 0x125, 0x126, 0x127, 0x128, 0x129, 0x12a, 0x12b, 0x12c, 0x12d, 0x12e, 0x12f,
		},
		{
			0x200, 0x201, 0x202, 0x203, 0x204, 0x205, 0x206, 0x207, 0x208, 0x209, 0x20a, 0x20b, 0x20c, 0x20d, 0x20e, 0x20f,
			0x210, 0x211, 0x212, 0x213, 0x214, 0x215, 0x216, 0x217, 0x218, 0x219, 0x21a, 0x21b, 0x21c, 0x21d, 0x21e, 0x21f,
			0x220, 0x221, 0x222, 0x223, 0x224, 0x225, 0x226, 0x227, 0x228, 0x229, 0x22a, 0x22b, 0x22c, 0x22d, 0x22e, 0x22f,
		},
		{
			0x300, 0x301, 0x302, 0x303, 0x304, 0x305, 0x306, 0x307, 0x308, 0x309, 0x30a, 0x30b, 0x30c, 0x30d, 0x30e, 0x30f,
			0x310, 0x311, 0x312, 0x313, 0x314, 0x315, 0x316, 0x317, 0x318, 0x319, 0x31a, 0x31b, 0x31c, 0x31d, 0x31e, 0x31f,
			0x320, 0x321, 0x322, 0x323, 0x324, 0x325, 0x326, 0x327, 0x328, 0x329, 0x32a, 0x32b, 0x32c, 0x32d, 0x32e, 0x32f,
		},
		{
			0x400, 0x401, 0x402, 0x403, 0x404, 0x405, 0x406, 0x407, 0x408, 0x409, 0x40a, 0x40b, 0x40c, 0x40d, 0x40e, 0x40f,
			0x410, 0x411, 0x412, 0x413, 0x414, 0x415, 0x416, 0x417, 0x418, 0x419, 0x41a, 0x41b, 0x41c, 0x41d, 0x41e, 0x41f,
			0x420, 0x421, 0x422, 0x423, 0x424, 0x425, 0x426, 0x427, 0x428, 0x429, 0x42a, 0x42b, 0x42c, 0x42d, 0x42e, 0x42f,
		},
		{
			0x500, 0x501, 0x502, 0x503, 0x504, 0x505, 0x506, 0x507, 0x508, 0x509, 0x50a, 0x50b, 0x50c, 0x50d, 0x50e, 0x50f,
			0x510, 0x511, 0x512, 0x513, 0x514, 0x515, 0x516, 0x517, 0x518, 0x519, 0x51a, 0x51b, 0x51c, 0x51d, 0x51e, 0x51f,
			0x520, 0x521, 0x522, 0x523, 0x524, 0x525, 0x526, 0x527, 0x528, 0x529, 0x52a, 0x52b, 0x52c, 0x52d, 0x52e, 0x52f,
		},
		{
			0x600, 0x601, 0x602, 0x603, 0x604, 0x605, 0x606, 0x607, 0x608, 0x609, 0x60a, 0x60b, 0x60c, 0x60d, 0x60e, 0x60f,
			0x610, 0x611, 0x612, 0x613, 0x614, 0x615, 0x616, 0x617, 0x618, 0x619, 0x61a, 0x61b, 0x61c, 0x61d, 0x61e, 0x61f,
			0x620, 0x621, 0x622, 0x623, 0x624, 0x625, 0x626, 0x627, 0x628, 0x629, 0x62a, 0x62b, 0x62c, 0x62d, 0x62e, 0x62f,
		},
		{
			0x700, 0x701, 0x702, 0x703, 0x704, 0x705, 0x706, 0x707, 0x708, 0x709, 0x70a, 0x70b, 0x70c, 0x70d, 0x70e, 0x70f,
			0x710, 0x711, 0x712, 0x713, 0x714, 0x715, 0x716, 0x717, 0x718, 0x719, 0x71a, 0x71b, 0x71c, 0x71d, 0x71e, 0x71f,
			0x720, 0x721, 0x722, 0x723, 0x724, 0x725, 0x726, 0x727, 0x728, 0x729, 0x72a, 0x72b, 0x72c, 0x72d, 0x72e, 0x72f,
		},
		{
			0x800, 0x801, 0x802, 0x803, 0x804, 0x805, 0x806, 0x807, 0x808, 0x809, 0x80a, 0x80b, 0x80c, 0x80d, 0x80e, 0x80f,
			0x810, 0x811, 0x812, 0x813, 0x814, 0x815, 0x816, 0x817, 0x818, 0x819, 0x81a, 0x81b, 0x81c, 0x81d, 0x81e, 0x81f,
			0x820, 0x821, 0x822, 0x823, 0x824, 0x825, 0x826, 0x827, 0x828, 0x829, 0x82a, 0x82b, 0x82c, 0x82d, 0x82e, 0x82f,
		},
		{
			0x900, 0x901, 0x902, 0x903, 0x904, 0x905, 0x906, 0x907, 0x908, 0x909, 0x90a, 0x90b, 0x90c, 0x90d, 0x90e, 0x90f,
			0x910, 0x911, 0x912, 0x913, 0x914, 0x915, 0x916, 0x917, 0x918, 0x919, 0x91a, 0x91b, 0x91c, 0x91d, 0x91e, 0x91f,
			0x920, 0x921, 0x922, 0x923, 0x924, 0x925, 0x926, 0x927, 0x928, 0x929, 0x92a, 0x92b, 0x92c, 0x92d, 0x92e, 0x92f,
		},
		{
			0xa00, 0xa01, 0xa02, 0xa03, 0xa04, 0xa05, 0xa06, 0xa07, 0xa08, 0xa09, 0xa0a, 0xa0b, 0xa0c, 0xa0d, 0xa0e, 0xa0f,
			0xa10, 0xa11, 0xa12, 0xa13, 0xa14, 0xa15, 0xa16, 0xa17, 0xa18, 0xa19, 0xa1a, 0xa1b, 0xa1c, 0xa1d, 0xa1e, 0xa1f,
			0xa20, 0xa21, 0xa22, 0xa23, 0xa24, 0xa25, 0xa26, 0xa27, 0xa28, 0xa29, 0xa2a, 0xa2b, 0xa2c, 0xa2d, 0xa2e, 0xa2f,
		},
		{
			0xb00, 0xb01, 0xb02, 0xb03, 0xb04, 0xb05, 0xb06, 0xb07, 0xb08, 0xb09, 0xb0a, 0xb0b, 0xb0c, 0xb0d, 0xb0e, 0xb0f,
			0xb10, 0xb11, 0xb12, 0xb13, 0xb14, 0xb15, 0xb16, 0xb17, 0xb18, 0xb19, 0xb1a, 0xb1b, 0xb1c, 0xb1d, 0xb1e, 0xb1f,
			0xb20, 0xb21, 0xb22, 0xb23, 0xb24, 0xb25, 0xb26, 0xb27, 0xb28, 0xb29, 0xb2a, 0xb2b, 0xb2c, 0xb2d, 0xb2e, 0xb2f,
		},
		{
			0xc00, 0xc01, 0xc02, 0xc03, 0xc04, 0xc05, 0xc06, 0xc07, 0xc08, 0xc09, 0xc0a, 0xc0b, 0xc0c, 0xc0d, 0xc0e, 0xc0f,
			0xc10, 0xc11, 0xc12, 0xc13, 0xc14, 0xc15, 0xc16, 0xc17, 0xc18, 0xc19, 0xc1a, 0xc1b, 0xc1c, 0xc1d, 0xc1e, 0xc1f,
			0xc20, 0xc21, 0xc22, 0xc23, 0xc24, 0xc25, 0xc26, 0xc27, 0xc28, 0xc29, 0xc2a, 0xc2b, 0xc2c, 0xc2d, 0xc2e, 0xc2f,
		},
		{
			0xd00, 0xd01, 0xd02, 0xd03, 0xd04, 0xd05, 0xd06, 0xd07, 0xd08, 0xd09, 0xd0a, 0xd0b, 0xd0c, 0xd0d, 0xd0e, 0xd0f,
			0xd10, 0xd11, 0xd12, 0xd13, 0xd14, 0xd15, 0xd16, 0xd17, 0xd18, 0xd19, 0xd1a, 0xd1b, 0xd1c, 0xd1d, 0xd1e, 0xd1f,
			0xd20, 0xd21, 0xd22, 0xd23, 0xd24, 0xd25, 0xd26, 0xd27, 0xd28, 0xd29, 0xd2a, 0xd2b, 0xd2c, 0xd2d, 0xd2e, 0xd2f,
		},
		{
			0xe00, 0xe01, 0xe02, 0xe03, 0xe04, 0xe05, 0xe06, 0xe07, 0xe08, 0xe09, 0xe0a, 0xe0b, 0xe0c, 0xe0d, 0xe0e, 0xe0f,
			0xe10, 0xe11, 0xe12, 0xe13, 0xe14, 0xe15, 0xe16, 0xe17, 0xe18, 0xe19, 0xe1a, 0xe1b, 0xe1c, 0xe1d, 0xe1e, 0xe1f,
			0xe20, 0xe21, 0xe22, 0xe23, 0xe24, 0xe25, 0xe26, 0xe27, 0xe28, 0xe29, 0xe2a, 0xe2b, 0xe2c, 0xe2d, 0xe2e, 0xe2f,
		},
		{
			0xf00, 0xf01, 0xf02, 0xf03, 0xf04, 0xf05, 0xf06, 0xf07, 0xf08, 0xf09, 0xf0a, 0xf0b, 0xf0c, 0xf0d, 0xf0e, 0xf0f,
			0xf10, 0xf11, 0xf12, 0xf13, 0xf14, 0xf15, 0xf16, 0xf17, 0xf18, 0xf19, 0xf1a, 0xf1b, 0xf1c, 0xf1d, 0xf1e, 0xf1f,
			0xf20, 0xf21, 0xf22, 0xf23, 0xf24, 0xf25, 0xf26, 0xf27, 0xf28, 0xf29, 0xf2a, 0xf2b, 0xf2c, 0xf2d, 0xf2e, 0xf2f,
		},
	}
)

func BenchmarkTransposeGo(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	x := matrix
	var z [16][16]uint32

	b.StartTimer()
	for range b.N {
		z[0][0] = x[0][0]
		z[0][1] = x[1][0]
		z[0][2] = x[2][0]
		z[0][3] = x[3][0]
		z[0][4] = x[4][0]
		z[0][5] = x[5][0]
		z[0][6] = x[6][0]
		z[0][7] = x[7][0]
		z[0][8] = x[8][0]
		z[0][9] = x[9][0]
		z[0][10] = x[10][0]
		z[0][11] = x[11][0]
		z[0][12] = x[12][0]
		z[0][13] = x[13][0]
		z[0][14] = x[14][0]
		z[0][15] = x[15][0]

		z[1][0] = x[0][1]
		z[1][1] = x[1][1]
		z[1][2] = x[2][1]
		z[1][3] = x[3][1]
		z[1][4] = x[4][1]
		z[1][5] = x[5][1]
		z[1][6] = x[6][1]
		z[1][7] = x[7][1]
		z[1][8] = x[8][1]
		z[1][9] = x[9][1]
		z[1][10] = x[10][1]
		z[1][11] = x[11][1]
		z[1][12] = x[12][1]
		z[1][13] = x[13][1]
		z[1][14] = x[14][1]
		z[1][15] = x[15][1]

		z[2][0] = x[0][2]
		z[2][1] = x[1][2]
		z[2][2] = x[2][2]
		z[2][3] = x[3][2]
		z[2][4] = x[4][2]
		z[2][5] = x[5][2]
		z[2][6] = x[6][2]
		z[2][7] = x[7][2]
		z[2][8] = x[8][2]
		z[2][9] = x[9][2]
		z[2][10] = x[10][2]
		z[2][11] = x[11][2]
		z[2][12] = x[12][2]
		z[2][13] = x[13][2]
		z[2][14] = x[14][2]
		z[2][15] = x[15][2]

		z[3][0] = x[0][3]
		z[3][1] = x[1][3]
		z[3][2] = x[2][3]
		z[3][3] = x[3][3]
		z[3][4] = x[4][3]
		z[3][5] = x[5][3]
		z[3][6] = x[6][3]
		z[3][7] = x[7][3]
		z[3][8] = x[8][3]
		z[3][9] = x[9][3]
		z[3][10] = x[10][3]
		z[3][11] = x[11][3]
		z[3][12] = x[12][3]
		z[3][13] = x[13][3]
		z[3][14] = x[14][3]
		z[3][15] = x[15][3]

		z[4][0] = x[0][4]
		z[4][1] = x[1][4]
		z[4][2] = x[2][4]
		z[4][3] = x[3][4]
		z[4][4] = x[4][4]
		z[4][5] = x[5][4]
		z[4][6] = x[6][4]
		z[4][7] = x[7][4]
		z[4][8] = x[8][4]
		z[4][9] = x[9][4]
		z[4][10] = x[10][4]
		z[4][11] = x[11][4]
		z[4][12] = x[12][4]
		z[4][13] = x[13][4]
		z[4][14] = x[14][4]
		z[4][15] = x[15][4]

		z[5][0] = x[0][5]
		z[5][1] = x[1][5]
		z[5][2] = x[2][5]
		z[5][3] = x[3][5]
		z[5][4] = x[4][5]
		z[5][5] = x[5][5]
		z[5][6] = x[6][5]
		z[5][7] = x[7][5]
		z[5][8] = x[8][5]
		z[5][9] = x[9][5]
		z[5][10] = x[10][5]
		z[5][11] = x[11][5]
		z[5][12] = x[12][5]
		z[5][13] = x[13][5]
		z[5][14] = x[14][5]
		z[5][15] = x[15][5]

		z[6][0] = x[0][6]
		z[6][1] = x[1][6]
		z[6][2] = x[2][6]
		z[6][3] = x[3][6]
		z[6][4] = x[4][6]
		z[6][5] = x[5][6]
		z[6][6] = x[6][6]
		z[6][7] = x[7][6]
		z[6][8] = x[8][6]
		z[6][9] = x[9][6]
		z[6][10] = x[10][6]
		z[6][11] = x[11][6]
		z[6][12] = x[12][6]
		z[6][13] = x[13][6]
		z[6][14] = x[14][6]
		z[6][15] = x[15][6]

		z[7][0] = x[0][7]
		z[7][1] = x[1][7]
		z[7][2] = x[2][7]
		z[7][3] = x[3][7]
		z[7][4] = x[4][7]
		z[7][5] = x[5][7]
		z[7][6] = x[6][7]
		z[7][7] = x[7][7]
		z[7][8] = x[8][7]
		z[7][9] = x[9][7]
		z[7][10] = x[10][7]
		z[7][11] = x[11][7]
		z[7][12] = x[12][7]
		z[7][13] = x[13][7]
		z[7][14] = x[14][7]
		z[7][15] = x[15][7]

		z[8][0] = x[0][8]
		z[8][1] = x[1][8]
		z[8][2] = x[2][8]
		z[8][3] = x[3][8]
		z[8][4] = x[4][8]
		z[8][5] = x[5][8]
		z[8][6] = x[6][8]
		z[8][7] = x[7][8]
		z[8][8] = x[8][8]
		z[8][9] = x[9][8]
		z[8][10] = x[10][8]
		z[8][11] = x[11][8]
		z[8][12] = x[12][8]
		z[8][13] = x[13][8]
		z[8][14] = x[14][8]
		z[8][15] = x[15][8]

		z[9][0] = x[0][9]
		z[9][1] = x[1][9]
		z[9][2] = x[2][9]
		z[9][3] = x[3][9]
		z[9][4] = x[4][9]
		z[9][5] = x[5][9]
		z[9][6] = x[6][9]
		z[9][7] = x[7][9]
		z[9][8] = x[8][9]
		z[9][9] = x[9][9]
		z[9][10] = x[10][9]
		z[9][11] = x[11][9]
		z[9][12] = x[12][9]
		z[9][13] = x[13][9]
		z[9][14] = x[14][9]
		z[9][15] = x[15][9]

		z[10][0] = x[0][10]
		z[10][1] = x[1][10]
		z[10][2] = x[2][10]
		z[10][3] = x[3][10]
		z[10][4] = x[4][10]
		z[10][5] = x[5][10]
		z[10][6] = x[6][10]
		z[10][7] = x[7][10]
		z[10][8] = x[8][10]
		z[10][9] = x[9][10]
		z[10][10] = x[10][10]
		z[10][11] = x[11][10]
		z[10][12] = x[12][10]
		z[10][13] = x[13][10]
		z[10][14] = x[14][10]
		z[10][15] = x[15][10]

		z[11][0] = x[0][11]
		z[11][1] = x[1][11]
		z[11][2] = x[2][11]
		z[11][3] = x[3][11]
		z[11][4] = x[4][11]
		z[11][5] = x[5][11]
		z[11][6] = x[6][11]
		z[11][7] = x[7][11]
		z[11][8] = x[8][11]
		z[11][9] = x[9][11]
		z[11][10] = x[10][11]
		z[11][11] = x[11][11]
		z[11][12] = x[12][11]
		z[11][13] = x[13][11]
		z[11][14] = x[14][11]
		z[11][15] = x[15][11]

		z[12][0] = x[0][12]
		z[12][1] = x[1][12]
		z[12][2] = x[2][12]
		z[12][3] = x[3][12]
		z[12][4] = x[4][12]
		z[12][5] = x[5][12]
		z[12][6] = x[6][12]
		z[12][7] = x[7][12]
		z[12][8] = x[8][12]
		z[12][9] = x[9][12]
		z[12][10] = x[10][12]
		z[12][11] = x[11][12]
		z[12][12] = x[12][12]
		z[12][13] = x[13][12]
		z[12][14] = x[14][12]
		z[12][15] = x[15][12]

		z[13][0] = x[0][13]
		z[13][1] = x[1][13]
		z[13][2] = x[2][13]
		z[13][3] = x[3][13]
		z[13][4] = x[4][13]
		z[13][5] = x[5][13]
		z[13][6] = x[6][13]
		z[13][7] = x[7][13]
		z[13][8] = x[8][13]
		z[13][9] = x[9][13]
		z[13][10] = x[10][13]
		z[13][11] = x[11][13]
		z[13][12] = x[12][13]
		z[13][13] = x[13][13]
		z[13][14] = x[14][13]
		z[13][15] = x[15][13]

		z[14][0] = x[0][14]
		z[14][1] = x[1][14]
		z[14][2] = x[2][14]
		z[14][3] = x[3][14]
		z[14][4] = x[4][14]
		z[14][5] = x[5][14]
		z[14][6] = x[6][14]
		z[14][7] = x[7][14]
		z[14][8] = x[8][14]
		z[14][9] = x[9][14]
		z[14][10] = x[10][14]
		z[14][11] = x[11][14]
		z[14][12] = x[12][14]
		z[14][13] = x[13][14]
		z[14][14] = x[14][14]
		z[14][15] = x[15][14]

		z[15][0] = x[0][15]
		z[15][1] = x[1][15]
		z[15][2] = x[2][15]
		z[15][3] = x[3][15]
		z[15][4] = x[4][15]
		z[15][5] = x[5][15]
		z[15][6] = x[6][15]
		z[15][7] = x[7][15]
		z[15][8] = x[8][15]
		z[15][9] = x[9][15]
		z[15][10] = x[10][15]
		z[15][11] = x[11][15]
		z[15][12] = x[12][15]
		z[15][13] = x[13][15]
		z[15][14] = x[14][15]
		z[15][15] = x[15][15]
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, z)
}

func BenchmarkTransposeAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	x := matrix
	var z [16][16]uint32

	xP := &[16]*[16]uint32{
		&x[0], &x[1], &x[2], &x[3], &x[4], &x[5], &x[6], &x[7],
		&x[8], &x[9], &x[10], &x[11], &x[12], &x[13], &x[14], &x[15],
	}

	b.StartTimer()
	for range b.N {
		Transpose(xP, &z)
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, z)
}

func BenchmarkTransposeAVXChunks(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	chunks := chunks
	var z [3][16][16]uint32

	chunk0 := &[16]*[16]uint32{
		(*[16]uint32)(unsafe.Pointer(&chunks[0][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[1][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[2][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[3][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[4][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[5][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[6][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[7][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[8][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[9][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[10][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[11][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[12][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[13][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[14][0])),
		(*[16]uint32)(unsafe.Pointer(&chunks[15][0])),
	}

	chunk1 := &[16]*[16]uint32{
		(*[16]uint32)(unsafe.Pointer(&chunks[0][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[1][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[2][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[3][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[4][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[5][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[6][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[7][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[8][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[9][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[10][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[11][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[12][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[13][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[14][16])),
		(*[16]uint32)(unsafe.Pointer(&chunks[15][16])),
	}

	chunk2 := &[16]*[16]uint32{
		(*[16]uint32)(unsafe.Pointer(&chunks[0][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[1][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[2][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[3][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[4][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[5][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[6][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[7][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[8][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[9][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[10][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[11][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[12][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[13][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[14][32])),
		(*[16]uint32)(unsafe.Pointer(&chunks[15][32])),
	}

	b.StartTimer()
	for range b.N {
		Transpose(chunk0, &z[0])
		Transpose(chunk1, &z[1])
		Transpose(chunk2, &z[2])
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, z)
}

func BenchmarkGGo(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	var va, vb, vc, vd, vmx, vmy [16]uint32

	randUint32Array(&va)
	randUint32Array(&vb)
	randUint32Array(&vc)
	randUint32Array(&vd)
	randUint32Array(&vmx)
	randUint32Array(&vmy)

	b.StartTimer()
	for range b.N {
		va[0], va[0], va[0], va[0] = g(va[0], va[0], va[0], va[0], va[0], va[0])
		va[1], va[1], va[1], va[1] = g(va[1], va[1], va[1], va[1], va[1], va[1])
		va[2], va[2], va[2], va[2] = g(va[2], va[2], va[2], va[2], va[2], va[2])
		va[3], va[3], va[3], va[3] = g(va[3], va[3], va[3], va[3], va[3], va[3])
		va[4], va[4], va[4], va[4] = g(va[4], va[4], va[4], va[4], va[4], va[4])
		va[5], va[5], va[5], va[5] = g(va[5], va[5], va[5], va[5], va[5], va[5])
		va[6], va[6], va[6], va[6] = g(va[6], va[6], va[6], va[6], va[6], va[6])
		va[7], va[7], va[7], va[7] = g(va[7], va[7], va[7], va[7], va[7], va[7])
		va[8], va[8], va[8], va[8] = g(va[8], va[8], va[8], va[8], va[8], va[8])
		va[9], va[9], va[9], va[9] = g(va[9], va[9], va[9], va[9], va[9], va[9])
		va[10], va[10], va[10], va[10] = g(va[10], va[10], va[10], va[10], va[10], va[10])
		va[11], va[11], va[11], va[11] = g(va[11], va[11], va[11], va[11], va[11], va[11])
		va[12], va[12], va[12], va[12] = g(va[12], va[12], va[12], va[12], va[12], va[12])
		va[13], va[13], va[13], va[13] = g(va[13], va[13], va[13], va[13], va[13], va[13])
		va[14], va[14], va[14], va[14] = g(va[14], va[14], va[14], va[14], va[14], va[14])
		va[15], va[15], va[15], va[15] = g(va[15], va[15], va[15], va[15], va[15], va[15])
	}
	b.StopTimer()
}

func BenchmarkGAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	var va, vb, vc, vd, vmx, vmy [16]uint32

	randUint32Array(&va)
	randUint32Array(&vb)
	randUint32Array(&vc)
	randUint32Array(&vd)
	randUint32Array(&vmx)
	randUint32Array(&vmy)

	b.StartTimer()
	for range b.N {
		G(&va, &vb, &vc, &vd, &vmx, &vmy)
	}

	b.StopTimer()
}

func BenchmarkAddGo(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]
	}
}

func BenchmarkAddAVX(b *testing.B) {
	x := x
	y := y

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
}

func BenchmarkAddAVX10(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		Add10(&x, &y, &x)
	}
}

func BenchmarkXorGo(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]
	}
}

func BenchmarkXorAVX(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
	}
}

func BenchmarkXorAVX10(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		Xor10(&x, &y, &x)
	}
}

func BenchmarkRotateRight7Go(b *testing.B) {
	const numOfBits = 7

	x := x

	for i := 0; i < b.N; i++ {
		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)
	}
}

func BenchmarkRotateRight7AVX(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
	}
}

func BenchmarkRotateRight7AVX10(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight107(&x, &x)
	}
}

func BenchmarkRotateRight8Go(b *testing.B) {
	const numOfBits = 8

	x := x

	for i := 0; i < b.N; i++ {
		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)
	}
}

func BenchmarkRotateRight8AVX(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
	}
}

func BenchmarkRotateRight8AVX10(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight108(&x, &x)
	}
}

func BenchmarkRotateRight12Go(b *testing.B) {
	const numOfBits = 12

	x := x

	for i := 0; i < b.N; i++ {
		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)
	}
}

func BenchmarkRotateRight12AVX(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
	}
}

func BenchmarkRotateRight12AVX10(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight1012(&x, &x)
	}
}

func BenchmarkRotateRight16Go(b *testing.B) {
	const numOfBits = 16

	x := x

	for i := 0; i < b.N; i++ {
		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)
	}
}

func BenchmarkRotateRight16AVX(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
	}
}

func BenchmarkRotateRight16AVX10(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight1016(&x, &x)
	}
}

func g(a, b, c, d, mx, my uint32) (uint32, uint32, uint32, uint32) {
	a += b + mx
	d = bits.RotateLeft32(d^a, -16)
	c += d
	b = bits.RotateLeft32(b^c, -12)
	a += b + my
	d = bits.RotateLeft32(d^a, -8)
	c += d
	b = bits.RotateLeft32(b^c, -7)
	return a, b, c, d
}

func randUint32Array(arr *[16]uint32) {
	_, err := rand.Read(unsafe.Slice((*byte)(unsafe.Pointer(&arr[0])), int(unsafe.Sizeof(arr[0]))*len(arr)))
	if err != nil {
		panic(err)
	}
}
