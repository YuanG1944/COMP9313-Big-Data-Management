#include<iostream>
#include<vector>

using namespace std;

void calculate(vector<vector<double>> & dp, int index, int k, long double R, long double N, long double B){
    if(index == 1){
        dp[index][k] = k * B - 2 * dp[2][k] - 3 * dp[3][k] - 4 * dp[4][k] - 5 * dp[5][k];
        return;
    }
    dp[index][k] = (6 - index) * dp[index - 1][k - 1] / (N - k + 1) 
                 + dp[index][k - 1] 
                 - (5 - index) * dp[index][k - 1] / (N - k + 1);
}

int main(){
    long double R = 20000000;
    long double N = 500;
    int K = 200;
    long double B = R / N;

    vector<vector<double>> dp(5 + 1, vector<double>(K + 1));
    dp[1][1] = B;

    for(int i = 2; i <= K; ++i){
        for(int j = 5; j > 0; --j){
            calculate(dp, j, i, R, N, B);
        }
    }
    // for(int i = 1; i < 6; ++i){
    //     for(int j = 1; j < 201; ++j){
    //         cout << dp[i][j] << " ";
    //     }
    //     putchar(10);
    // }
    cout << dp[5][200] << endl;
}