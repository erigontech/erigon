#include "utils.h"

int lcp_kasai(const unsigned char *T, int *SA, int *LCP, int *FTR, int *INV, int sa_size, int n)
{
    for (int i = 0, j = 0; i < sa_size; i++)
    {
        if ((SA[i] & 1) == 0)
            FTR[j++] = SA[i] >> 1;
    }

    for (int i = 0; i < n; i++)
        INV[FTR[i]] = i;

    for (int i = 0, k = 0; i < n; i++, k ? k-- : 0)
    {
        if (INV[i] == n - 1)
        {
            k = 0;
            continue;
        }

        int j = FTR[INV[i] + 1];

        while (i + k < n && j + k < n && (int)T[(i + k) * 2] != 0 &&
               (int)T[(j + k) * 2] != 0 && T[(i + k) * 2 + 1] == T[(j + k) * 2 + 1])
            k++;

        LCP[INV[i]] = k;
    }

    return 0;
}
