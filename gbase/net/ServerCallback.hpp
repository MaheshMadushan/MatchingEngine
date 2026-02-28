template <typename T, typename R>
class ServerCallback
{
private:
    T /* data */

public:
    ServerCallback(T /* args */);

    virtual R onData(T data) = 0;
};
