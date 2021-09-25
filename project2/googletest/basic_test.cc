#include <gtest/gtest.h>
#include <cmath>
#include <vector>

int Factorial(int n){
    if(n==0) return 1;
    int ret = 1;
    for(int i=2;i<=n;i++) ret *= i;
    return ret;
}

bool IsPrime(int n){
    if(n<2) return false;
    for(int i=2;i<=sqrt(n);i++){
        if(n%i==0) return false;
    }
    return true;
}

TEST(FactorialTest, HandlesZeroInput){
    EXPECT_EQ(Factorial(0), 1);
}