#ifndef __glToolkit_h__
#define __glToolkit_h__

#define GLEW_NO_GLU
#include <GL/glew.h>
#include <glm/vec4.hpp>

#include <functional>

#if defined(WITH_SDL)
#elif defined(WITH_GLFW)
#else
#error "Neither WITH_GLFW nor WITH_SDL specified - no available windowing support!"
#endif

#ifdef WITH_GLFW
class GLFWwindow;
#endif

#ifdef WITH_SDL
class SDL_Window;
#endif

// describes a super-sampled framebuffer
struct SSDescriptor {
	enum {
		SS_4X,	// 2x2
		SS_9X,	// 3x3
		SS_16X	// 4x4
	} mode;		// select a supersampling mode
	bool forcePowerOfTwoFramebuffer = false;	// true to force create a framebuffer that has power-of-two width and height;
												// use this if the runtime system doesn't support non-power-of-two render targets
	unsigned framebufferFormat = GL_RGB8;		// the internal pixel format to use for the render target

	// returns the linear super sampling factor (how many samples per pixel in X or Y direction)
	unsigned getLinearSampleFactor() const {
		if (mode == SS_4X)
			return 2;
		else if (mode == SS_9X)
			return 3;
		else if (mode == SS_16X)
			return 4;
		else {
			assert("invalid super sampling mode!");
			return 1;
		}
	}
};

enum class PostProcessStep {
	PRE_DOWNSAMPLING,
	POST_DOWNSAMPLING
};

#ifdef WITH_GLFW
// initializes GLFW, openGL an' all
// if multisampleCount is non-zero, multi-sampling antialiasing (MSAA) will be enabled
bool gltInitGLFW(unsigned windowWidth=512, unsigned windowHeight=512, const char windowTitle[]="Untitled", unsigned multiSampleCount=0);

// initializes openGL and create a supersampled framebuffer (SSAA)
bool gltInitGLFWSupersampled(unsigned windowWidth, unsigned windowHeight, SSDescriptor desc, const char windowTitle[]="Untitled");

GLFWwindow* gltGetWindow();
#endif

#ifdef WITH_SDL
// initialize openGL on an SDL window
bool gltInitSDL(SDL_Window* window);

// initialize openGL on an SDL window and create a supersampled framebuffer (SSAA)
bool gltInitSDLSupersampled(SDL_Window* window, SSDescriptor desc);
#endif

// begins a frame
void gltBegin(glm::vec4 clearColor = glm::vec4{0});

// finishes a frame and displays the result
void gltEnd();


// returns true if SS is enabled and fills the provided buffer with data; returns false otherwise
bool gltGetSuperSampleInfo(SSDescriptor& outDesc);

// Allows the caller to set up to two post-processing hooks, one that is executed before the downsampling step
// (on the raw super-sampled framebuffer - if supersampling is enabled),
// and the second after downsampling, on the screen-sized framebuffer.
// If supersampling is turned off, the pre-downsample hook will have no effect.
// In the pre-downsampling step, the draw framebuffer will have the same size as the supersampled framebuffer.
// In the post-downsampling step, the draw framebuffer has the same size as the screen framebuffer.
// Usually it's preferable to do postprocessing only after downsampling since it will be faster (fewer pixels), and
// it consumes less memory (no additional full-supersampled framebuffer needs to be created).
//
// The framebuffer texture to be used as input is bound to GL_TEXTURE0 GL_TEXTURE_2D target before the callback is invoked.
// The viewport is also correctly set to cover the entire target framebuffer prior to calling the callback.
// The user is responsible for all other aspects of the post-process rendering - screen quad, shader etc.
void gltSetPostProcessHook(PostProcessStep step, std::function<void()> hook);

// checks if an OpenGL error has occured and prints it on stderr if so;
// returns true if error, false if no error
bool checkGLError(const char* operationName = nullptr);

#ifdef WITH_SDL
// checks if an SDL error has occured and prints it on stderr if so;
// returns true if error, false if no error
bool checkSDLError(const char* operationName = nullptr);
#endif

#endif //__glToolkit_h__
