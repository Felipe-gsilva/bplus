#ifndef _APP
#define _APP

#include "defines.h"

void cli(app *a);

app *alloc_app(void);

void clear_app(app *app);

int main(int argc, char **argv);

#endif
